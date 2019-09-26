package codekey

import "bytes"

//根据分隔符和要拼接的字符串拼接key
func EncodeKey(seq string, args ...string) *bytes.Buffer {
	b := bytes.Buffer{}
	for _, arg := range args[:len(args)-1] {
		b.WriteString(arg)
		b.WriteString(seq)
	}
	b.WriteString(args[len(args)-1])
	return &b
}
