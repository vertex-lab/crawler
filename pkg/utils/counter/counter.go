// The package counter defines a minimalistic Float counter
package counter

import (
	"sync/atomic"
)

const defaultScale float64 = 1000000

// Float is a struct for a floating point counter. The value of the counter is
// counter.Load() / scale.
type Float struct {
	scale   float64
	counter atomic.Int64
}

// NewFloatCounter() returns a new Float counter with the specified scale factor (which controls precision).
func NewFloatCounter() *Float {
	return &Float{
		scale:   defaultScale,
		counter: atomic.Int64{},
	}
}

// Add() increases the counter by delta and returns the current value.
func (c *Float) Add(delta float64) float64 {
	if c == nil {
		return 0
	}

	incr := int64(delta*c.scale + 0.5)
	c.counter.Add(incr)
	return float64(c.counter.Load()) / c.scale
}

// Load() returns the current value.
func (c *Float) Load() float64 {
	if c == nil {
		return 0
	}
	return float64(c.counter.Load()) / c.scale
}

// Store() overwrites the current value to val.
func (c *Float) Store(val float64) {
	if c == nil {
		return
	}

	intVal := int64(val*c.scale + 0.5)
	c.counter.Store(intVal)
}
