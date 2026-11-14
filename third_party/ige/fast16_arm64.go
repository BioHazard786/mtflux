//go:build arm64

package ige

// useFast16 enables the register-chained 16-byte IGE loops. Measured on an
// Ampere A1 (Neoverse N1): 644 vs 525 MB/s decrypting 1 MiB. On amd64 the
// original loop is faster (960 vs 820 MB/s), so it stays the default there.
const useFast16 = true
