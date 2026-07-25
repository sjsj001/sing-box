package group

import "github.com/sagernet/sing-box/option"

// smart_region.go resolves the static region model from config and annotates a
// candidate with its region, mode, and primary flag so the selection flow's
// region rules (§3 step 2) can run. See tingly-riding-parrot-final.md §0/§3.

type regionModel struct {
	region  map[string]string // node tag -> region name
	mode    map[string]string // region name -> "prefer" | "equivalent"
	primary map[string]string // region name -> primary node tag
}

func buildRegionModel(regions []option.SmartRegionOptions) *regionModel {
	m := &regionModel{
		region:  make(map[string]string),
		mode:    make(map[string]string),
		primary: make(map[string]string),
	}
	for _, r := range regions {
		mode := r.Mode
		if mode == "" {
			mode = "equivalent"
		}
		m.mode[r.Name] = mode
		if r.Primary != "" {
			m.primary[r.Name] = r.Primary
		}
		for _, member := range r.Members {
			m.region[member] = r.Name
		}
	}
	return m
}

// annotate fills the region fields of a candidate in place.
func (m *regionModel) annotate(c *nodeCandidate) {
	region, ok := m.region[c.tag]
	if !ok {
		return
	}
	c.region = region
	c.regionMode = m.mode[region]
	c.isPrimary = m.primary[region] == c.tag
}
