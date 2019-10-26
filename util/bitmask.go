package util

type BitMask uint32

func (f BitMask) Has(flag BitMask) bool { return f&flag != 0 }
func (f *BitMask) Set(flag BitMask)     { *f |= flag }
func (f *BitMask) Unset(flag BitMask)   { *f &= ^flag }
func (f *BitMask) Toggle(flag BitMask)  { *f ^= flag }
