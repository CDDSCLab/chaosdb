package comparator

import (
	"fmt"
	"testing"
)

func TestLevelDBComparator_Compare(t *testing.T) {
	c := StringAndNumberComparator{}

	a := c.Compare([]byte("t_r_1_1"), []byte("t_r_1_10"))
	fmt.Println(a)
	b := c.Compare([]byte("t_r_a_b"), []byte("t_r_a_c"))
	fmt.Println(b)
	d := c.Compare([]byte("t_r_1"), []byte("t_i_1_100"))
	fmt.Println(d)
	e := c.Compare([]byte("t_r_1"), []byte("t_r_1"))
	fmt.Println(e)
	f := c.Compare([]byte("t_r_a"), []byte("t_r_a"))
	fmt.Println(f)
}
