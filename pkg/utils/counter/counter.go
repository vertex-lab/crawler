package counter

import (
	"sync/atomic"
)

// Float is a struct for a floating point counter. counter / scale is the value it holds.
type Float struct {
	scale   float64
	counter atomic.Int64
}

// NewFloatCounter() returns a new Float counter with the specified scale factor (which controls precision).
// The value hold inside the counter is conter.Load() / scale (which must non-zero).
func NewFloatCounter(scale float64) *Float {
	if scale <= 0 {
		return nil
	}

	return &Float{
		scale:   scale,
		counter: atomic.Int64{},
	}
}

// Add increases the counter by delta and returns the current value.
func (c *Float) Add(delta float64) float64 {
	if c == nil {
		return 0
	}

	incr := int64(delta*c.scale + 0.5)
	c.counter.Add(incr)
	return float64(c.counter.Load()) / c.scale
}

// Load returns the current value.
func (c *Float) Load() float64 {
	if c == nil {
		return 0
	}
	return float64(c.counter.Load()) / c.scale
}

// Store overwrites the current value to val.
func (c *Float) Store(val float64) {
	if c == nil {
		return
	}

	intVal := int64(val*c.scale + 0.5)
	c.counter.Store(intVal)
}
