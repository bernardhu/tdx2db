package database

// GPJYVALUE()，股票交易数据
// GPJYONE()
var gpbase = []cwColumnDesc{
	//-------------每股指标-----------------------------
	{idx: 0, name: "f0", desc: "基本每股收益", scale: 1.0},
	{idx: 1, name: "f1", desc: "扣除非经常性损益每股收益", scale: 1.0},
	{idx: 2, name: "f2", desc: "每股未分配利润", scale: 1.0},
	{idx: 3, name: "f3", desc: "每股净资产", scale: 1.0},
	{idx: 4, name: "f4", desc: "每股资本公积金", scale: 1.0},
	{idx: 5, name: "f5", desc: "净资产收益率", scale: 1.0},
	{idx: 6, name: "f6", desc: "每股经营现金流量", scale: 1.0},
	//-------------资产负债表----------------------------
	{idx: 7, name: "f7", desc: "货币资金", scale: 1.0},
	{idx: 8, name: "f8", desc: "交易性金融资产(万元)", scale: 1.0},
	{idx: 9, name: "f9", desc: "应收票据", scale: 1.0},
	{idx: 10, name: "f10", desc: "应收账款", scale: 1.0},
	{idx: 11, name: "f11", desc: "预付款项", scale: 1.0},
	{idx: 12, name: "f12", desc: "其他应收款", scale: 1.0},
	{idx: 13, name: "f13", desc: "应收关联公司款", scale: 1.0},
	{idx: 14, name: "f14", desc: "应收利息", scale: 1.0},
	{idx: 15, name: "f15", desc: "应收股利", scale: 1.0},
	{idx: 16, name: "f16", desc: "存货", scale: 1.0},
	{idx: 17, name: "f17", desc: "其中：消耗性生物资产", scale: 1.0},
	{idx: 18, name: "f18", desc: "一年内到期的非流动资产", scale: 1.0},
	{idx: 19, name: "f19", desc: "其他流动资产", scale: 1.0},
	{idx: 20, name: "f20", desc: "流动资产合计", scale: 1.0},
}
