#!/usr/bin/env python3
"""Read a smart-outbound audit trail and report the decisions that do not look right.

The trail records what every group brought to each decision, so the checks here
are the ones a person would do by hand and get wrong: did the choice follow from
the numbers on the line, and are the numbers themselves plausible.
"""
import collections
import datetime
import json
import re
import sys

# Has to match the outbound. switchHysteresis is what stops two near-equal
# groups trading places on noise; raceHardTimeout bounds chasing a better answer.
SWITCH_HYSTERESIS_MS = 15.0
RACE_HARD_TIMEOUT_MS = 500.0
FLAP_WINDOW_S = 600
# What the deadline model leaves out of an answer: the proxy resolves DNS before
# it dials (one to three milliseconds warm, around fifty cold — and cold is when
# races run), and a cold pool pays its setup first. Grace for both, so a floor
# derived from silence stays on the honest side of the model's gap.
COLD_SLACK_MS = 60.0

UNITS = {"ns": 1e-6, "us": 1e-3, "µs": 1e-3, "ms": 1.0, "s": 1000.0, "m": 60000.0, "h": 3600000.0}
PART = re.compile(r"([0-9.]+)(ns|us|µs|ms|s|m|h)")


def ms(text):
    """Parse a Go duration into milliseconds. Returns None when absent.

    The sign is carried by the whole duration, not by its parts: Go formats
    -1.5s as "-1.5s" and a compound one as "-1m30s", with the minus appearing
    once at the front. Scores are routinely negative — score is
    alpha*local + remote - bonus, so any group whose bonus exceeds its
    discounted local scores below zero, and so does its bound. Reading those as
    positive inverts every comparison this tool makes about exactly the groups
    a bonus was configured for.
    """
    if not text:
        return None
    total, matched = 0.0, False
    for value, unit in PART.findall(text):
        total += float(value) * UNITS[unit]
        matched = True
    if not matched:
        return None
    return -total if text.lstrip().startswith("-") else total


def when(record):
    """The record's own timestamp."""
    return stamp_of(record.get("time", ""))


def stamp_of(stamp):
    """Parse a Go RFC3339Nano timestamp.

    Go trims trailing zeros from the fraction and drops it entirely on a whole
    second, so "…:05.12+08:00" and "…:05+08:00" are both ordinary output — a
    fixed-width slice plus a fixed format string rejected all but the ~1e-6 of
    stamps whose fraction happens to be nine significant digits, and silently
    turned every flap check into "nothing to report".
    """
    if not stamp:
        return None
    # fromisoformat before 3.11 rejects 'Z' for UTC; rewrite it to the numeric
    # offset every version accepts.
    text = stamp.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.datetime.fromisoformat(text)
    except ValueError:
        # fromisoformat before 3.11 rejects more than six fractional digits.
        trimmed = re.sub(r"\.(\d{6})\d+", r".\1", text)
        try:
            parsed = datetime.datetime.fromisoformat(trimmed)
        except ValueError:
            return None
    # Comparisons and subtractions across the trail must not mix aware and
    # naive stamps; normalising to UTC also keeps a DST change from reordering
    # the moves either side of it.
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(datetime.timezone.utc).replace(tzinfo=None)
    return parsed


def legs(record):
    return {g["tag"]: g for g in record.get("groups", [])}


def race_index(races):
    """Races by destination key, so a later record can be read against the round
    that produced it."""
    index = collections.defaultdict(list)
    for r in races:
        stamp = when(r)
        if stamp is not None:
            index[r.get("key")].append((stamp, r))
    for rounds in index.values():
        rounds.sort(key=lambda row: row[0])
    return index


def round_behind(index, key, raced_at):
    """The round a snapshot came out of: the last one at or before its raced_at.

    A round that answers in two parts writes the trail twice, so the match has to
    be the latest round not after the snapshot rather than an exact stamp.
    """
    rounds = index.get(key)
    if not rounds:
        return None
    marker = stamp_of(raced_at)
    if marker is None:
        return rounds[-1][1]
    # A race that hands out a provisional answer stamps raced_at when the caller
    # leaves; its background half writes the round's audit record up to the
    # answer grace later. The slack lets a round's stamp sit slightly after the
    # raced_at that points at it.
    marker += datetime.timedelta(seconds=1)
    behind = [record for at, record in rounds if at <= marker]
    return behind[-1] if behind else None


def floor_score(group, elapsed):
    """The lowest score a group that never answered could plausibly have had.

    The group's own bound will not do. bound is alpha*local - bonus, which is the
    score it would get if the destination leg were free — and no real destination
    is. Against a group that did answer, that comparison is not evidence of
    anything: any nearby group clears it for almost any destination, so a check
    built on it reports most of the table.

    What the silence does support is a floor. A group that had not answered when
    the round ended has local + remote > elapsed under the deadline model, and its
    score is alpha*local + remote - bonus, which is bound + remote — so the score
    sits above bound + (elapsed - local). The model is tight, not exact: a real
    answer arrives at local + span plus a cold pool's setup, so the silence term
    is deflated by COLD_SLACK_MS to stay on the honest side of that gap, at the
    price of flagging a little more.
    """
    bound, local = ms(group.get("bound")), ms(group.get("local"))
    if bound is None or local is None or elapsed is None:
        return None
    return bound + max(0.0, elapsed - local - COLD_SLACK_MS)


def resolve_names(records):
    """Fill in the destinations a dump could not name.

    The outbound only knows what it has dialed since it started, so a dump taken
    after a restart reports most of a restored cache under its digest. The round
    that put each of those in the cache named it, and that record is still here.
    """
    names = {}
    for r in records:
        if r.get("key") and r.get("destination"):
            names[r["key"]] = r["destination"]
        for held in r.get("destinations") or ():
            if held.get("key") and held.get("destination"):
                names[held["key"]] = held["destination"]
    for r in records:
        if not r.get("destination") and r.get("key"):
            r["destination"] = names.get(r["key"], r["key"])
    return records


def load(path):
    records, broken = [], 0
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except Exception:
                broken += 1
    return records, broken


def section(title):
    print()
    print("=" * 78)
    print(title)
    print("=" * 78)


def report(findings, limit=6):
    if not findings:
        print("  正常")
        return
    for line in findings[:limit]:
        print("  " + line)
    if len(findings) > limit:
        print(f"  ... 另有 {len(findings) - limit} 条")


def check_races(races):
    section(f"初始化（race）：{len(races)} 场")

    # Trails written before the outbound learned to flag an inherited value have
    # to be recognised by the value repeating unchanged from the previous race
    # for the same destination and group. On a trail that carries the flag the
    # guess is not only unnecessary but wrong: two rounds may legitimately
    # measure the same microsecond.
    flagged = any(g.get("cached") for r in races for g in r.get("groups", []))
    previous = {}

    def stale(destination, tag, remote):
        if flagged or remote is None:
            return False
        key = (destination, tag)
        was = previous.get(key)
        previous[key] = remote
        return was == remote

    answered = collections.Counter()
    failed = collections.Counter()
    reasons = collections.Counter()
    absent = collections.Counter()
    seen = collections.Counter()
    wrong_winner, slow, alone, no_winner = [], [], [], []

    for r in races:
        table = legs(r)
        winner = r.get("to") or ""
        elapsed = ms(r.get("elapsed"))
        # Only groups that answered in this round. A cached score is the
        # snapshot's, and counting it would say a group that never took part
        # should have won — which is exactly the false conclusion the "cached"
        # flag was added to prevent. Trails written before that flag existed are
        # recognised by the value repeating unchanged from the previous race.
        scored = {tag: ms(g.get("score")) for tag, g in table.items()
                  if g.get("score") is not None and not g.get("cached")
                  and not stale(r.get("destination"), tag, g.get("remote"))}

        for tag, g in table.items():
            seen[tag] += 1
            if g.get("score") is not None:
                answered[tag] += 1
            elif g.get("failed"):
                failed[tag] += 1
                reasons[(tag, g["failed"][:60])] += 1
            elif g.get("local") is None:
                absent[tag] += 1

        if not winner:
            no_winner.append(f"{r.get('destination')}  没有任何组胜出")
            continue
        if scored:
            best = min(scored, key=scored.get)
            if winner != best:
                wrong_winner.append(
                    f"{r.get('destination')}  选了 {winner}={scored.get(winner)}ms"
                    f"  但 {best}={scored[best]}ms 更低")
        if len(scored) <= 1:
            alone.append(f"{r.get('destination')}  只有 {list(scored) or '零'} 组答复，没有可比性")
        if elapsed is not None and elapsed >= RACE_HARD_TIMEOUT_MS:
            unmeasured = [t for t, g in table.items() if g.get("local") is None]
            slow.append(f"{r.get('destination')}  耗时 {elapsed}ms（撞上限）"
                        f"  未测量的组: {unmeasured or '无'}")

    print("\n[各组答复率]")
    for tag in sorted(seen, key=lambda t: -seen[t]):
        total = seen[tag]
        rate = 100.0 * answered[tag] / total if total else 0
        note = ""
        if rate < 20:
            note = "  <== 几乎从不参与排名"
        print(f"  {tag:4s} 出现 {total:4d}  答复 {answered[tag]:4d} ({rate:5.1f}%)"
              f"  失败 {failed[tag]:4d}  无测量 {absent[tag]:4d}{note}")

    if reasons:
        print("\n[失败原因]")
        for (tag, reason), count in reasons.most_common(8):
            print(f"  {count:4d}  {tag:4s}  {reason}")

    print("\n[胜者不是最低分]")
    report(wrong_winner)
    print("\n[无人胜出]")
    report(no_winner)
    print("\n[只有一个组答复]")
    report(alone)
    print(f"\n[耗时撞上 {RACE_HARD_TIMEOUT_MS:.0f}ms 硬上限] 共 {len(slow)} 场")
    report(slow)


def check_switches(switches, races):
    section(f"切换（switch）：{len(switches)} 次")
    if not switches:
        print("  没有切换记录")
        return

    regressions, thin, locked, listing = [], [], [], []
    unjudged = 0
    history = collections.defaultdict(list)
    index = race_index(races)

    for r in switches:
        table = legs(r)
        destination, src, dst = r.get("destination"), r.get("from"), r.get("to")
        reason = r.get("reason", "")
        from_score = ms(table.get(src, {}).get("score"))
        to_score = ms(table.get(dst, {}).get("score"))
        stamp = when(r)
        history[destination].append((stamp, src, dst))

        listing.append(f"{destination}  {src or '(空)'} -> {dst}  by={reason}"
                       f"  {src}={from_score}ms {dst}={to_score}ms")

        if from_score is not None and to_score is not None:
            if to_score > from_score:
                regressions.append(f"{destination}  切到了更差的一组："
                                   f"{src}={from_score}ms -> {dst}={to_score}ms")
            elif from_score - to_score < SWITCH_HYSTERESIS_MS and reason == "score":
                thin.append(f"{destination}  只好 {from_score - to_score:.1f}ms"
                            f"（< 迟滞 {SWITCH_HYSTERESIS_MS}ms）就切了")

        # A group that could still have won but carries no destination measurement
        # is not selectable at all: it never answered, so the snapshot has no
        # entry, and nothing can put it back until the snapshot expires. Judged
        # against what its silence supports rather than against its bound — see
        # floor_score for why the bound is not evidence.
        source = round_behind(index, r.get("key"), r.get("raced_at") or r.get("time"))
        elapsed = ms(source.get("elapsed")) if source else None
        source_legs = legs(source) if source else {}
        if to_score is not None:
            for tag, g in table.items():
                if g.get("score") is not None:
                    continue
                floor = floor_score(source_legs.get(tag, g), elapsed)
                if floor is None:
                    unjudged += 1
                elif floor < to_score - SWITCH_HYSTERESIS_MS:
                    locked.append(f"{destination}  选了 {dst}={to_score}ms，但 {tag} 的沉默只证明"
                                  f"它不低于 {floor:.1f}ms —— 仍可能更优，却因为没有测量值而没资格被选")

    print("\n[全部切换]")
    report(listing, limit=20)
    print("\n[切到了分数更差的组]")
    report(regressions)
    print("\n[改善量小于迟滞阈值]")
    report(thin)
    print("\n[可能更优的组因为没有测量值而没资格参选]")
    report(locked, limit=10)
    if unjudged:
        print(f"  （另有 {unjudged} 处缺当轮 race 记录或该组的 local/bound，未做判断）")

    print("\n[同一目标反复横跳]")
    flaps = []
    for destination, moves in history.items():
        moves.sort(key=lambda m: (m[0] is None, m[0]))
        for i in range(1, len(moves)):
            before, after = moves[i - 1], moves[i]
            if before[0] and after[0] and (after[0] - before[0]).total_seconds() <= FLAP_WINDOW_S:
                if after[2] == before[1]:
                    flaps.append(f"{destination}  {(after[0] - before[0]).total_seconds():.0f}s 内"
                                 f" {before[1]} -> {before[2]} -> {after[2]}")
    report(flaps)


def check_members(members):
    section(f"组内成员切换（member）：{len(members)} 次")
    if not members:
        print("  没有成员切换")
        return
    by_group = collections.defaultdict(list)
    for r in members:
        group = (r.get("groups") or [{}])[0].get("tag", "?")
        by_group[group].append((when(r), r.get("from"), r.get("to"), r.get("reason")))

    for group, moves in sorted(by_group.items()):
        moves.sort(key=lambda m: (m[0] is None, m[0]))
        print(f"\n  [{group}] {len(moves)} 次")
        for stamp, src, dst, reason in moves[:8]:
            print(f"    {stamp}  {src} -> {dst}  by={reason}")
        if len(moves) > 8:
            print(f"    ... 另有 {len(moves) - 8} 次")
        flaps = sum(1 for i in range(1, len(moves))
                    if moves[i][2] == moves[i - 1][1]
                    and moves[i][0] and moves[i - 1][0]
                    and (moves[i][0] - moves[i - 1][0]).total_seconds() <= FLAP_WINDOW_S)
        if flaps:
            print(f"    <== 其中 {flaps} 次是在 {FLAP_WINDOW_S}s 内来回横跳")


def check_state(states, races):
    section("当前状态（state）：最后一次快照")
    if not states:
        print("  没有状态记录")
        return
    index = race_index(races)
    latest = max(r.get("time", "") for r in states)
    current = [r for r in states if r.get("time") == latest]
    print(f"  时刻 {latest}，共 {len(current)} 个目标")

    chosen = collections.Counter(r.get("to") or "(无)" for r in current)
    print("\n[选择分布]")
    for tag, count in chosen.most_common():
        print(f"  {tag:6s} {count:5d}  ({100.0 * count / len(current):5.1f}%)")

    locked, mismatched, unjudged = [], [], 0
    for r in current:
        table = legs(r)
        picked = r.get("to")
        picked_score = ms(table.get(picked, {}).get("score"))
        if picked_score is None:
            continue
        # The round this snapshot came out of, for the one thing the snapshot
        # cannot say on its own: how long the groups that stayed quiet were given.
        source = round_behind(index, r.get("key"), r.get("raced_at"))
        elapsed = ms(source.get("elapsed")) if source else None
        source_legs = legs(source) if source else {}
        for tag, g in table.items():
            if tag == picked:
                continue
            if g.get("score") is not None:
                if ms(g["score"]) < picked_score:
                    mismatched.append(f"{r.get('destination')}  在用 {picked}={picked_score}ms"
                                      f"，但 {tag}={ms(g['score'])}ms 更低")
                continue
            # The silence belongs to the round, so the floor is read from the
            # round's own record of the group where there is one — the dump's
            # local is whatever the member measures now, a different epoch.
            floor = floor_score(source_legs.get(tag, g), elapsed)
            if floor is None:
                # Missing round, or a leg with no local/bound to derive from.
                # Saying nothing beats reporting on evidence that proves nothing.
                unjudged += 1
                continue
            # A measurement could only promote the group past the incumbent by
            # clearing the same hysteresis a switch needs, so a thinner gap is
            # not actionable either.
            if floor < picked_score - SWITCH_HYSTERESIS_MS:
                locked.append((picked_score - floor, r.get("destination"), picked,
                               picked_score, tag, floor))

    destinations = len({row[1] for row in locked})
    print(f"\n[可能更优的组没有测量值、因而没资格被选] {destinations} 个目标（{len(locked)} 处）")
    locked.sort(key=lambda row: row[0], reverse=True)
    for gap, destination, picked, picked_score, tag, floor in locked[:12]:
        print(f"  {destination}")
        print(f"      在用 {picked} 分数 {picked_score}ms；{tag} 的沉默只证明它不低于 {floor:.1f}ms"
              f"——仍可能更优（最多可省 {gap:.1f}ms），却没有测量值、没资格被选")
    if len(locked) > 12:
        print(f"  ... 另有 {len(locked) - 12} 处")
    if unjudged:
        print(f"  （另有 {unjudged} 处缺当轮 race 记录或该组的 local/bound，未做判断）")

    print(f"\n[选择与已有分数不符] {len({m.split('  ')[0] for m in mismatched})} 个目标"
          f"（{len(mismatched)} 处）")
    report(mismatched, limit=10)

    # 判定这个目标的那一轮，某个组当时是由另一个成员顶着的 —— 也就是说这条结论
    # 是关于那个成员的，而现在跑的是别的节点。这类快照会在下次拨号时重新比较。
    drifted = []
    for r in current:
        for tag, g in legs(r).items():
            if g.get("raced_by"):
                drifted.append(f"{r.get('destination')}  在用 {r.get('to')}；"
                               f"{tag} 当时由 {g['raced_by']} 顶着，现在是 {g.get('member')}")
    print(f"\n[判定时某组正在故障切换中] {len(drifted)} 处")
    report(drifted, limit=10)


def selftest():
    """Pin the floor semantics: python3 smart_audit_check.py --selftest.

    Three trails, one per way the locked check can go. The point of keeping them
    executable is the failure mode this check had before: a criterion that reads
    plausibly and reports the entire table.
    """
    def grp(tag, local, bound, score=None):
        g = {"tag": tag, "member": "m", "local": local, "bound": bound}
        if score is not None:
            g["score"] = score
        return g

    def race(key, elapsed):
        return {"time": "2026-01-01T01:00:00.0+08:00", "type": "race", "key": key,
                "destination": key + ".example:443", "to": "us", "elapsed": elapsed,
                "groups": [grp("us", "120ms", "84ms", "200ms"), grp("jp", "10ms", "5ms")]}

    def state(key):
        return {"time": "2026-01-01T02:00:00.0+08:00", "type": "state", "key": key,
                "destination": key + ".example:443", "to": "us",
                "raced_at": "2026-01-01T01:00:00.1+08:00",
                "groups": [grp("us", "120ms", "84ms", "200ms"), grp("jp", "10ms", "5ms")]}

    import io
    from contextlib import redirect_stdout

    def run(states, races):
        out = io.StringIO()
        with redirect_stdout(out):
            check_state(states, races)
        return out.getvalue()

    # A round that ran long: the silence proves jp slow, so nothing is reported.
    long_round = run([state("aaa")], [race("aaa", "400ms")])
    assert "0 个目标（0 处）" in long_round, long_round
    # A round cut short while jp was unmeasured: its silence proves nearly
    # nothing, so it is exactly the locked-out-though-possibly-better case.
    cut_short = run([state("bbb")], [race("bbb", "50ms")])
    assert "1 个目标（1 处）" in cut_short, cut_short
    assert "沉默只证明它不低于" in cut_short, cut_short
    # No round to read the silence against: counted, not guessed at.
    orphan = run([state("ccc")], [])
    assert "0 个目标（0 处）" in orphan and "未做判断" in orphan, orphan
    print("selftest ok")


def main():
    if "--selftest" in sys.argv[1:]:
        selftest()
        return
    path = sys.argv[1] if len(sys.argv) > 1 else "/etc/sing-box/audit.jsonl"
    records, broken = load(path)
    print(f"{path}: {len(records)} 条记录，{broken} 条无法解析")

    by_type = collections.defaultdict(list)
    for r in records:
        by_type[r.get("type")].append(r)

    resolve_names(records)
    check_races(by_type.get("race", []))
    check_switches(by_type.get("switch", []), by_type.get("race", []))
    check_members(by_type.get("member", []))
    check_state(by_type.get("state", []), by_type.get("race", []))


if __name__ == "__main__":
    main()
