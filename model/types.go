package model

import "time"

type DBConfig struct {
	Path string
}

type DayfileRecord struct {
	Date   uint32
	Open   uint32
	High   uint32
	Low    uint32
	Close  uint32
	Amount float32
	Volume uint32
}

type MinfileRecord struct {
	DateRaw uint16
	TimeRaw uint16
	Open    uint32
	High    uint32
	Low     uint32
	Close   uint32
	Amount  float32
	Volume  uint32
}

type StockData struct {
	Symbol string
	Open   float64
	High   float64
	Low    float64
	Close  float64
	Amount float64
	Volume int64
	Date   time.Time
}

type Factor struct {
	Symbol    string
	Date      time.Time
	Close     float64
	PreClose  float64
	QfqFactor float64
	HfqFactor float64
}

type GbbqData struct {
	Category int
	Code     string
	Date     time.Time
	C1       float64
	C2       float64
	C3       float64
	C4       float64
}

type XdxrData struct {
	Code        string
	Date        time.Time
	Fenhong     float64
	Peigujia    float64
	Songzhuangu float64
	Peigu       float64
}

type CapitalData struct {
	Code            string
	Date            time.Time
	PrevOutstanding float64
	PrevTotal       float64
	Outstanding     float64
	Total           float64
}

type DbfRecord struct {
	Code string
	Mkt  string
	ZGB  float64 //总股本 *10000
	BG   float64 //b股 *10000
	HG   float64 //流通H股 *10000
	LTAG float64 //流通A股 *10000

	CQTZ   uint64  //股东人数
	SSDATE uint64  //上市日期
	TZMGJZ float64 //每股净资产
	ZGG    float64 //每股收益

	//三大表
	//利润表
	ZYSY float64 //利润表 营业总收入 *1000
	ZYLY float64 //利润表 营业成本 *1000
	YYLY float64 //利润表 营业利润 *1000
	LYZE float64 //利润表 利润总额*1000
	SHLY float64 //利润表 净利润*1000
	JLY  float64 //利润表 归母净利润*1000

	//现金流量表
	BTSY  float64 //现金流量表 经营活动产生的现金流量净额 *1000
	YYWSZ float64 //总现金流=经营现金流+投资现金流+筹资现金流 *1000

	//勾稽关系
	//ldzc+fldzc(自己算)=zzc=ldfz+fldfx(自己算)+jzc+cqfz
	//资产负债表
	//流动资产
	LDZC   float64 //资产负债表 流动资产合计*1000
	SNSYTZ float64 ///资产负债表 存货*1000
	//非流动资产
	GDZC float64 //资产负债表 固定资产*1000
	WXZC float64 //资产负债表 无形资产*1000
	ZZC  float64 //资产负债表 总资产*1000  = 负债+所有者权益
	//流动负债
	LDFZ float64 //资产负债表 流动负债*1000
	QTLY float64 //资产负债表 应收账款*1000
	//非流动负债

	//所有者权益 = 归母+少数
	JZC  float64 //资产负债表 归母所有者权益*1000
	CQFZ float64 //资产负债表 少数股东权益*1000

	WFPLY float64 //资产负债表 未分配利润*1000
	ZBGJJ float64 //资产负债表 资本公积金*1000

	ZBNB uint64 //中报年报

}
