package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"time"
)

func main() {
	cmd := Cmd{}
	flag.BoolVar(&cmd.Scale, "scale", true, "Scale sampled values to represent estimates of the true allocations.")
	flag.IntVar(&cmd.Exp, "exp", 8, "Repeat workload 10^exp times.")
	flag.Int64Var(&cmd.Seed, "seed", time.Now().UnixNano(), "Seed for randomn number generator.")
	flag.BoolVar(&cmd.Errors, "errors", false, "Report errors relative to perfect profiler.")
	flag.Parse()
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}
}

type Cmd struct {
	Scale  bool
	Exp    int
	Seed   int64
	Errors bool
}

func (c *Cmd) Run() error {
	var (
		newRand = func() *rand.Rand { return rand.New(rand.NewSource(c.Seed)) }
		rate    = 100 * 1024
		small   = 16
		big     = 128
	)

	var (
		profilers = []func(scale bool) Profiler{
			func(scale bool) Profiler { return &PerfectProfiler{} },
			func(scale bool) Profiler { return &DotNetProfiler{Scale: scale, Rate: rate} },
			// func(scale bool) Profiler { return &GoProfiler{Scale: scale, Rand: newRand(), Rate: rate} },
		}
		workloads = []func() Workload{
			func() Workload { return SequentialWorkload{Small: small, Big: big} },
			func() Workload { return InterleaveWorkload{Small: small, Big: big} },
			func() Workload { return InterleaveWorkload{Small: small, Big: big, Rand: newRand()} },
			func() Workload { return SequentialWorkload{Small: small, Big: rate * 2} },
			func() Workload { return InterleaveWorkload{Small: small, Big: rate * 2} },
			func() Workload { return InterleaveWorkload{Small: small, Big: rate * 2, Rand: newRand()} },
		}
	)

	var results []Result
	var resultIdx = ResultIndex{}
	ops := int64(math.Pow10(c.Exp))
	for _, newProfiler := range profilers {
		for _, newWorkload := range workloads {
			profiler := newProfiler(c.Scale)
			workload := newWorkload()
			workload.Work(ops, profiler)
			profile := profiler.Profile()
			key := ResultKey{Workload: workload.Name(), Profiler: profiler.Name()}
			resultIdx[key] = profile
			results = append(results, Result{ResultKey: key, Profile: profile})
		}
	}

	cw := csv.NewWriter(os.Stdout)
	defer cw.Flush()

	cw.Write([]string{"profiler", "workload", "stack", "objects", "bytes"})

	perfect := results[0].Profiler
	for _, r := range results {
		if c.Errors && r.Profiler == perfect {
			continue
		}

		sortedStacks := []string{}
		for st := range r.Profile {
			sortedStacks = append(sortedStacks, string(st))
		}
		sort.Strings(sortedStacks)

		for _, st := range sortedStacks {
			st := StackTrace(st)
			objects := fmt.Sprintf("%d", r.Profile[st].Objects)
			bytes := fmt.Sprintf("%d", r.Profile[st].Bytes)
			if c.Errors {
				perfectResult := resultIdx[ResultKey{Workload: r.Workload, Profiler: perfect}][st]
				objects = errorPercent(float64(r.Profile[st].Objects), float64(perfectResult.Objects))
				bytes = errorPercent(float64(r.Profile[st].Bytes), float64(perfectResult.Bytes))
			}

			cw.Write([]string{
				r.Profiler,
				r.Workload,
				string(st),
				objects,
				bytes,
			})
		}
	}

	return nil
}

type Profiler interface {
	Name() string
	Malloc(size int, stack StackTrace)
	Profile() Profile
}

// PerfectProfiler records every allocation and reports the results.
type PerfectProfiler struct {
	prof Profile
}

func (p *PerfectProfiler) Name() string { return "perfect" }

func (p *PerfectProfiler) Malloc(size int, stack StackTrace) {
	p.prof.Add(stack, Alloc{Objects: 1, Bytes: int64(size)})
}
func (p *PerfectProfiler) Profile() Profile { return p.prof }

// DotNetProfiler records one allocation every Rate bytes. The resulting
// profile is scaled by 1/(size/rate) to estimate the true allocations.
type DotNetProfiler struct {
	Scale bool
	Rate  int

	nextSample int
	prof       Profile
}

func (p *DotNetProfiler) Name() string { return "dotnet" }

func (p *DotNetProfiler) Malloc(size int, stack StackTrace) {
	if size < p.nextSample {
		p.nextSample -= size
	} else {
		p.prof.Add(stack, Alloc{Objects: 1, Bytes: int64(size)})
		p.nextSample = p.Rate
	}
}
func (p *DotNetProfiler) Profile() Profile {
	if !p.Scale {
		return p.prof
	}
	scaled := p.prof.Copy()
	for st, v := range scaled {
		avgSize := float64(v.Bytes) / float64(v.Objects)
		scale := 1 / (float64(avgSize) / float64(p.Rate))

		scaled[st] = Alloc{
			Objects: int64(float64(v.Objects) * scale),
			Bytes:   int64(float64(v.Bytes) * scale),
		}
	}
	return scaled
}

// GoProfiler records an allocation and then draws a random sampling distance
// in bytes for the next allocation from the exponential distribution with a
// mean of Rate. The resulting profile is scaled by 1 / (1 - e^(-size/rate))
// to estimate the true allocations.
type GoProfiler struct {
	Scale bool
	Rand  *rand.Rand
	Rate  int

	nextSample int
	prof       Profile
}

func (p *GoProfiler) Name() string { return "go" }

func (p *GoProfiler) Malloc(size int, stack StackTrace) {
	if size < p.nextSample {
		p.nextSample -= size
	} else {
		p.prof.Add(stack, Alloc{Objects: 1, Bytes: int64(size)})
		p.nextSample = int(float64(p.Rate) * p.Rand.ExpFloat64())
		// code above produces the same result as:
		//p.nextSample = int(-math.Log(1-p.Rand.Float64()) / (1 / float64(p.Rate)))
	}
}
func (p *GoProfiler) Profile() Profile {
	if !p.Scale {
		return p.prof
	}
	scaled := p.prof.Copy()
	for st, v := range scaled {
		avgSize := float64(v.Bytes) / float64(v.Objects)
		scale := 1 / (1 - math.Exp(-avgSize/float64(p.Rate)))

		scaled[st] = Alloc{
			Objects: int64(float64(v.Objects) * scale),
			Bytes:   int64(float64(v.Bytes) * scale),
		}
	}
	return scaled
}

type Profile map[StackTrace]Alloc

func (p *Profile) Add(stack StackTrace, alloc Alloc) {
	if *p == nil {
		*p = Profile{}
	}
	update := (*p)[stack]
	update.Objects += alloc.Objects
	update.Bytes += alloc.Bytes
	(*p)[stack] = update
}

func (p Profile) Copy() Profile {
	copy := make(Profile, len(p))
	for st, v := range p {
		copy[st] = v
	}
	return copy
}

type Alloc struct {
	Objects int64
	Bytes   int64
}

type StackTrace string

type Workload interface {
	Name() string
	Work(ops int64, p Profiler)
}

type InterleaveWorkload struct {
	Small int
	Big   int
	Rand  *rand.Rand
}

func (w InterleaveWorkload) Name() string {
	rand := ""
	if w.Rand != nil {
		rand = "-rand"
	}
	return fmt.Sprintf("interlave%s-%d-%d", rand, w.Small, w.Big)
}

func (w InterleaveWorkload) Work(ops int64, p Profiler) {
	for i := int64(0); i < ops; i++ {
		if w.Rand == nil || w.Rand.Float64() < 0.5 {
			p.Malloc(w.Small, "small")
		}
		if w.Rand == nil || w.Rand.Float64() < 0.5 {
			p.Malloc(w.Big, "big")
		}
	}
}

type SequentialWorkload struct {
	Small int
	Big   int
}

func (w SequentialWorkload) Name() string {
	return fmt.Sprintf("sequential-%d-%d", w.Small, w.Big)
}

func (w SequentialWorkload) Work(ops int64, p Profiler) {
	for i := int64(0); i < ops; i++ {
		p.Malloc(w.Small, "small")
	}
	for i := int64(0); i < ops; i++ {
		p.Malloc(w.Big, "big")
	}
}

type Result struct {
	ResultKey
	Profile Profile
}

type ResultKey struct {
	Workload string
	Profiler string
}

type ResultIndex map[ResultKey]Profile

func errorPercent(got, want float64) string {
	return fmt.Sprintf("%.2f%%", (got-want)/want*100)
}
