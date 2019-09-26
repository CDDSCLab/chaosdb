package comparator

import (
	"bytes"
	"strconv"

	"github.com/CDDSCLab/chaosdb/opt/common"
	"github.com/CDDSCLab/chaosdb/util/stringutil"
)

//自己定义的比较器
//用来解决入库时键的排序问题
//例子：
//t_r_uxto_1=>[{...,...}] t_r_uxto_2=>[{...,...}] t_r_uxto_10=>[{...,...}]
//按照默认比较器排序(ASCⅡ的大小)结果如下：
//t_r_uxto_1=>[{...,...}] t_r_uxto_10=>[{...,...}] t_r_uxto_2=>[{...,...}]
//自定义比较器逻辑：按照固定字符拆分键值，如果是字符，按照ASCII比较大小，如果是数字(48~57) 转换为10进制数字进行比较
type StringAndNumberComparator struct {
}

func (l *StringAndNumberComparator) Compare(a, b []byte) int {

	//拆分键
	bs1 := bytes.Split(a, []byte(common.Separator))
	bs2 := bytes.Split(b, []byte(common.Separator))
	//长度不同，直接按照ascii排序
	if len(bs1) != len(bs2) {
		return bytes.Compare(a, b)
	}

	for i := 0; i < len(bs1); i++ {
		//长度相同，试着转换为数字
		num1, err1 := strconv.Atoi(stringutil.Bytes2str(bs1[i]))
		num2, err2 := strconv.Atoi(stringutil.Bytes2str(bs2[i]))
		if err1 == nil && err2 == nil {
			//如果成功转换，并且当前数字可以比较出大小，直接返回结果
			//如果当前数字相等，继续比较一下个，直到可以比较出大小
			if num1 > num2 {
				return 1
			} else if num1 < num2 {
				return -1
			} else {
				continue
			}
		} else {
			//如果转换不成功，并且根据字符大小可以得出大小，直接返回
			//如果字符相等，继续比较
			cmpbyte := bytes.Compare(bs1[i], bs2[i])
			if cmpbyte != 0 {
				return cmpbyte
			}
			continue
		}
	}

	//如果循环内无法判断出大小，说明两个键相等。
	return 0

}

func (l *StringAndNumberComparator) Name() string {
	return "StringAndNumberComparator"
}

func (l *StringAndNumberComparator) Separator(dst, a, b []byte) []byte {
	return nil
}

func (l *StringAndNumberComparator) Successor(dst, b []byte) []byte {
	return nil
}
