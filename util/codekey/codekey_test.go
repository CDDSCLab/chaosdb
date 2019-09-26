package codekey

import (
	"fmt"
	"testing"
)

func TestEncodeKey(t *testing.T) {
	b := EncodeKey("_", "t", "r", "1", "1", "aa", "bb", "cc")
	fmt.Println(b.String())
}
