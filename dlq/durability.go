package dlq

// Durability says what an acknowledged Append survives.
type Durability int

const (
	// DurabilityUnknown is a store that does not say. Treat it as not durable.
	DurabilityUnknown Durability = iota
	// DurabilityVolatile loses everything when the process exits (MemoryStore).
	DurabilityVolatile
	// DurabilityBuffered survives a process crash but can lose the last moments of
	// acknowledged work to a power failure or kernel crash (a WALStore with
	// SyncInterval or SyncNone).
	DurabilityBuffered
	// DurabilityDurable survives a crash and a power failure: the data was on
	// stable storage before the call returned (a WALStore with SyncAlways).
	DurabilityDurable
)

func (d Durability) String() string {
	switch d {
	case DurabilityVolatile:
		return "volatile"
	case DurabilityBuffered:
		return "buffered"
	case DurabilityDurable:
		return "durable"
	}
	return "unknown"
}

// DurabilityReporter is implemented by stores that state their durability. A
// custom Store should implement it so that consumers configured to require a
// durable store (kafka.Config.RequireDurableStore) accept it.
type DurabilityReporter interface{ Durability() Durability }

// StoreDurability returns what s reports about itself, or DurabilityUnknown.
func StoreDurability(s Store) Durability {
	if r, ok := s.(DurabilityReporter); ok {
		return r.Durability()
	}
	return DurabilityUnknown
}

// Durability implements DurabilityReporter.
func (w *WALStore) Durability() Durability {
	if w.opts.Sync == SyncAlways {
		return DurabilityDurable
	}
	return DurabilityBuffered
}

// Durability implements DurabilityReporter.
func (s *MemoryStore) Durability() Durability { return DurabilityVolatile }

// Durability implements DurabilityReporter: sealing does not change it.
func (s *secureStore) Durability() Durability { return StoreDurability(s.inner) }
