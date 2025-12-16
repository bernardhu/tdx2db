-- =====================================
-- 1. 三大报表原始+单季核心
-- =====================================
CREATE OR REPLACE VIEW v_cw_stmt_core AS
SELECT
    code,      -- 证券代码
    mkt,       -- 市场
    rdate,     -- 报告期
    rpt_type,  -- 报告类型

    ----------------- 每股 / ROE 核心 -----------------
    f0   AS eps_basic,                 -- 基本每股收益
    f1   AS eps_basic_excl_extra,      -- 扣除非经常性损益每股收益
    f2   AS retained_earnings_ps,      -- 每股未分配利润
    f3   AS net_assets_ps,             -- 每股净资产
    f4   AS capital_reserve_ps,        -- 每股资本公积金
    f5   AS roe,                       -- 净资产收益率
    f6   AS operating_cf_ps,           -- 每股经营现金流量

    ----------------- 资产负债表（通用） -----------------
    -- 资产负债表：资产 流动/非流动资产到资产总计
    f7   AS monetary_funds,                          -- 货币资金
    f8   AS trading_financial_assets,                -- 交易性金融资产
    f9   AS notes_receivable,                        -- 应收票据
    f10  AS accounts_receivable,                     -- 应收账款
    f11  AS prepayments,                             -- 预付款项
    f12  AS other_receivables,                       -- 其他应收款
    f13  AS receivables_from_related_parties,        -- 应收关联公司款
    f14  AS interest_receivable,                     -- 应收利息
    f15  AS dividends_receivable,                    -- 应收股利
    f16  AS inventories,                             -- 存货
    f17  AS consumptive_bio_assets,                  -- 其中：消耗性生物资产
    f18  AS noncurrent_assets_due_within_1y,         -- 一年内到期的非流动资产
    f19  AS other_current_assets,                    -- 其他流动资产
    f20  AS total_current_assets,                    -- 流动资产合计
    f21  AS afs_financial_assets,                    -- 可供出售金融资产
    f22  AS held_to_maturity_investments,            -- 持有至到期投资
    f23  AS long_term_receivables,                   -- 长期应收款
    f24  AS long_term_equity_investments,            -- 长期股权投资
    f25  AS investment_properties,                   -- 投资性房地产
    f26  AS fixed_assets,                            -- 固定资产
    f27  AS construction_in_progress,                -- 在建工程
    f28  AS construction_materials,                  -- 工程物资
    f29  AS fixed_assets_disposal,                   -- 固定资产清理
    f30  AS productive_bio_assets,                   -- 生产性生物资产
    f31  AS oil_gas_assets,                          -- 油气资产
    f32  AS intangible_assets,                       -- 无形资产
    f33  AS development_expenditure,                 -- 开发支出
    f34  AS goodwill,                                -- 商誉
    f35  AS long_term_prepaid_expenses,              -- 长期待摊费用
    f36  AS deferred_tax_assets,                     -- 递延所得税资产
    f37  AS other_noncurrent_assets,                 -- 其他非流动资产
    f38  AS total_noncurrent_assets,                 -- 非流动资产合计
    f39  AS total_assets,                            -- 资产总计

    -- 资产负债表：负债（流动 + 非流动）到负债合计
    f40  AS short_term_borrowings,                   -- 短期借款
    f41  AS trading_financial_liabilities,           -- 交易性金融负债
    f42  AS notes_payable,                           -- 应付票据
    f43  AS accounts_payable,                        -- 应付账款
    f44  AS advances_from_customers,                 -- 预收款项
    f45  AS payroll_payable,                         -- 应付职工薪酬
    f46  AS taxes_payable,                           -- 应交税费
    f47  AS interest_payable,                        -- 应付利息
    f48  AS dividends_payable,                       -- 应付股利
    f49  AS other_payables,                          -- 其他应付款
    f50  AS payables_to_related_parties,             -- 应付关联公司款
    f51  AS noncurrent_liabilities_due_within_1y,    -- 一年内到期的非流动负债
    f52  AS other_current_liabilities,               -- 其他流动负债
    f53  AS total_current_liabilities,               -- 流动负债合计
    f54  AS long_term_borrowings,                    -- 长期借款
    f55  AS bonds_payable,                           -- 应付债券
    f56  AS long_term_payables,                      -- 长期应付款
    f57  AS specific_payables,                       -- 专项应付款
    f58  AS provisions,                              -- 预计负债
    f59  AS deferred_tax_liabilities,                -- 递延所得税负债
    f60  AS other_noncurrent_liabilities,            -- 其他非流动负债
    f61  AS total_noncurrent_liabilities,            -- 非流动负债合计
    f62  AS total_liabilities,                       -- 负债合计

    -- 资产负债表：权益 所有者权益到“负债和所有者权益合计”
    f63  AS share_capital,                           -- 实收资本（或股本）
    f64  AS capital_reserve,                         -- 资本公积
    f65  AS surplus_reserve,                         -- 盈余公积
    f66  AS treasury_shares,                         -- 减：库存股
    f67  AS retained_earnings,                       -- 未分配利润
    f68  AS minority_interests,                      -- 少数股东权益
    f69  AS fx_translation_reserve,                  -- 外币报表折算价差
    f70  AS abnormal_items_adjustment,               -- 非正常经营项目收益调整
    f71  AS total_equity,                            -- 所有者权益（或股东权益）合计
    f72  AS total_liabilities_and_equity,            -- 负债和所有者（或股东权益）合计

    -- 新口径资产负债表补充
    f270 AS equity_attributable_to_owners,           -- 归属于母公司股东权益
    f294 AS notes_and_accounts_payable,              -- 应付票据及应付账款
    f295 AS notes_and_accounts_receivable,           -- 应收票据及应收账款
    f296 AS deferred_income,                         -- 递延收益
    f297 AS other_comprehensive_income,              -- 其他综合收益
    f298 AS other_equity_instruments,                -- 其他权益工具

    ----------------- 利润表（全年口径） -----------------
    -- 利润表 营业收入/成本/费用/三费/利润总额/净利润等
    f73  AS operating_revenue,                       -- 营业收入
    f74  AS operating_cost,                          -- 营业成本
    f75  AS business_taxes_and_surcharges,           -- 营业税金及附加
    f76  AS selling_expenses,                        -- 销售费用
    f77  AS administrative_expenses,                 -- 管理费用
    f78  AS exploration_expenses,                    -- 勘探费用
    f79  AS finance_expenses,                        -- 财务费用
    f80  AS asset_impairment_losses,                 -- 资产减值损失
    f81  AS fair_value_gain,                         -- 公允价值变动净收益
    f82  AS investment_income,                       -- 投资收益
    f83  AS income_from_associates_and_joint_ventures, -- 对联营企业和合营企业的投资收益
    f84  AS other_items_affecting_operating_profit,  -- 影响营业利润的其他科目
    f85  AS operating_profit,                        -- 营业利润
    f86  AS subsidy_income,                          -- 补贴收入
    f87  AS non_operating_income,                    -- 营业外收入
    f88  AS non_operating_expenses,                  -- 营业外支出
    f89  AS loss_on_disposal_of_noncurrent_assets,   -- 非流动资产处置净损失
    f90  AS other_items_affecting_total_profit,      -- 影响利润总额的其他科目
    f91  AS total_profit,                            -- 利润总额
    f92  AS income_tax_expense,                      -- 所得税
    f93  AS other_items_affecting_net_profit,        -- 影响净利润的其他科目
    f94  AS net_profit,                              -- 净利润
    f95  AS net_profit_attributable_to_owners,       -- 归属于母公司股东的净利润
    f96  AS profit_attributable_to_minority_interests, -- 少数股东损益

    -- 利润表新增：处置收益 / 持续经营 / 终止经营 / 研发费用
    f300 AS gain_on_disposal_of_assets,              -- 资产处置收益
    f301 AS net_profit_from_continuing_operations,   -- 持续经营净利润
    f302 AS net_profit_from_discontinued_operations, -- 终止经营净利润
    f303 AS r_and_d_expenses,                        -- 研发费用

    ----------------- 单季度利润/现金流核心 -----------------
    -- 单季度营业收入/营业利润/归母净利/扣非净利
    f229 AS operating_revenue_q,                     -- 营业收入（单季度）
    f230 AS operating_profit_q,                      -- 营业利润（单季度）
    f231 AS net_profit_attributable_to_owners_q,     -- 归母净利润（单季度）
    f232 AS net_profit_excl_extra_q,                 -- 扣非净利润（单季度）
    -- 单季度经营/投资/筹资现金流、现金净增加额
    f233 AS net_cash_from_operating_activities_q,    -- 经营活动产生的现金流量净额（单季度）
    f234 AS net_cash_from_investing_activities_q,    -- 投资活动产生的现金流量净额（单季度）
    f235 AS net_cash_from_financing_activities_q,    -- 筹资活动产生的现金流量净额（单季度）
    f236 AS net_increase_in_cash_eq_q,               -- 现金及现金等价物净增加额（单季度）
    -- 单季度 EPS
    f310 AS eps_basic_q,                             -- 基本每股收益（单季度）
    
    ----------------- 现金流量表（全年口径） -----------------
    -- 主表：经营/投资/筹资流入流出及净额到期末现金余额
    f97  AS cash_received_from_sales_and_services,             -- 销售商品、提供劳务收到的现金
    f98  AS tax_refunds_received,                              -- 收到的税费返还
    f99  AS other_operating_cash_inflows,                      -- 收到其他与经营活动有关的现金
    f100 AS total_operating_cash_inflows,                      -- 经营活动现金流入小计
    f101 AS cash_paid_for_goods_and_services,                  -- 购买商品、接受劳务支付的现金
    f102 AS cash_paid_to_and_on_behalf_of_employees,           -- 支付给职工以及为职工支付的现金
    f103 AS cash_paid_for_taxes,                               -- 支付的各项税费
    f104 AS other_operating_cash_outflows,                     -- 支付其他与经营活动有关的现金
    f105 AS total_operating_cash_outflows,                     -- 经营活动现金流出小计
    f106 AS net_cash_from_operating_activities,                -- 经营活动产生的现金流量净额

    f107 AS cash_received_from_disposal_of_investments,        -- 收回投资收到的现金
    f108 AS cash_received_from_investment_income,              -- 取得投资收益收到的现金
    f109 AS cash_received_from_disposal_of_long_term_assets,   -- 处置固定资产、无形资产和其他长期资产收回的现金净额
    f110 AS net_cash_received_from_disposal_of_subsidiaries,   -- 处置子公司及其他营业单位收到的现金净额
    f111 AS other_investing_cash_inflows,                      -- 收到其他与投资活动有关的现金
    f112 AS total_investing_cash_inflows,                      -- 投资活动现金流入小计
    f113 AS cash_paid_for_acquisition_of_long_term_assets,     -- 购建固定资产、无形资产和其他长期资产支付的现金
    f114 AS cash_paid_for_investments,                         -- 投资支付的现金
    f115 AS net_cash_paid_for_acquisition_of_subsidiaries,     -- 取得子公司及其他营业单位支付的现金净额
    f116 AS other_investing_cash_outflows,                     -- 支付其他与投资活动有关的现金
    f117 AS total_investing_cash_outflows,                     -- 投资活动现金流出小计
    f118 AS net_cash_from_investing_activities,                -- 投资活动产生的现金流量净额

    f119 AS cash_received_from_investors,                      -- 吸收投资收到的现金
    f120 AS cash_received_from_borrowings,                     -- 取得借款收到的现金
    f121 AS other_financing_cash_inflows,                      -- 收到其他与筹资活动有关的现金
    f122 AS total_financing_cash_inflows,                      -- 筹资活动现金流入小计
    f123 AS cash_repaid_for_debts,                             -- 偿还债务支付的现金
    f124 AS cash_paid_for_dividends_and_interest,              -- 分配股利、利润或偿付利息支付的现金
    f125 AS other_financing_cash_outflows,                     -- 支付其他与筹资活动有关的现金
    f126 AS total_financing_cash_outflows,                     -- 筹资活动现金流出小计
    f127 AS net_cash_from_financing_activities,                -- 筹资活动产生的现金流量净额
    f128 AS effect_of_fx_changes_on_cash,                      -- 汇率变动对现金的影响
    f129 AS effect_of_other_changes_on_cash,                   -- 其他原因对现金的影响
    f130 AS net_increase_in_cash_eq,                           -- 现金及现金等价物净增加额
    f131 AS cash_eq_at_beginning_of_period,                    -- 期初现金及现金等价物余额
    f132 AS cash_eq_at_end_of_period,                          -- 期末现金及现金等价物余额

    -- 现金流补充资料：折旧摊销、减值准备等
    f133 AS net_profit_cf,                                     -- 净利润（现金流量表补充）
    f134 AS provision_for_asset_impairment,                    -- 资产减值准备
    f135 AS depreciation_and_depletion,                        -- 固定资产折旧、油气资产折耗、生产性生物资产折旧
    f136 AS amortisation_of_intangibles,                       -- 无形资产摊销
    f137 AS amortisation_of_long_term_prepaid_expenses,        -- 长期待摊费用摊销
    f138 AS loss_on_disposal_of_long_term_assets,              -- 处置固定资产、无形资产和其他长期资产的损失
    f139 AS loss_on_retirement_of_fixed_assets,                -- 固定资产报废损失
    f140 AS loss_from_changes_in_fair_value,                   -- 公允价值变动损失
    f141 AS finance_costs_cf,                                  -- 财务费用（补充资料）
    f142 AS investment_losses,                                 -- 投资损失
    f143 AS decrease_in_deferred_tax_assets,                   -- 递延所得税资产减少
    f144 AS increase_in_deferred_tax_liabilities,              -- 递延所得税负债增加
    f145 AS decrease_in_inventories,                           -- 存货的减少
    f146 AS decrease_in_operating_receivables,                 -- 经营性应收项目的减少
    f147 AS increase_in_operating_payables,                    -- 经营性应付项目的增加
    f148 AS other_cf_adjustments,                              -- 其他（现金流量调整）
    f149 AS net_cash_from_operating_activities_alt,            -- 经营活动产生的现金流量净额2
    f150 AS debt_to_capital,                                   -- 债务转为资本
    f151 AS convertible_bonds_due_within_1y,                   -- 一年内到期的可转换公司债券
    f152 AS finance_leased_fixed_assets,                       -- 融资租入固定资产
    f153 AS cash_ending_balance,                               -- 现金的期末余额
    f154 AS cash_beginning_balance,                            -- 现金的期初余额
    f155 AS cash_equivalents_ending_balance,                   -- 现金等价物的期末余额
    f156 AS cash_equivalents_beginning_balance,                -- 现金等价物的期初余额
    f157 AS net_increase_in_cash_and_equivalents,              -- 现金及现金等价物净增加额

    -- 近一年滚动指标 & 自由现金流
    f306 AS ocf_last_12m,                                      -- 近一年经营活动现金流净额
    f307 AS net_profit_attributable_to_owners_last_12m,        -- 近一年归母净利润（万元）
    f308 AS net_profit_excl_extra_last_12m,                    -- 近一年扣非净利润（万元）
    f309 AS net_cash_flow_last_12m,                            -- 近一年现金净流量（万元）
    f315 AS icf_last_12m,                                      -- 近一年投资活动现金流净额(万元)

    ----------------- 自由现金流/每股现金流 -----------------
    f320 AS free_cf_to_firm_ps,        -- 每股企业自由现金流
    f321 AS free_cf_to_equity_ps       -- 每股股东自由现金流 
FROM raw_caiwu;


-- =====================================
-- 2. 现金流结构与质量 比率、盈利质量、成长性 因子
-- =====================================
CREATE OR REPLACE VIEW v_cw_ratio_quality AS
SELECT
    code,
    mkt,
    rdate,
    rpt_type,

    ----------------- 偿债能力 / 杠杆 -----------------
    f158 AS current_ratio,                         -- 流动比率
    f159 AS quick_ratio,                           -- 速动比率
    f160 AS cash_ratio,                            -- 现金比率(%)
    f161 AS interest_coverage_ratio,               -- 利息保障倍数
    f162 AS noncurrent_liability_ratio,            -- 非流动负债比率(%)
    f163 AS current_liability_ratio,               -- 流动负债比率(%)
    f164 AS cash_to_maturing_debt_ratio,           -- 现金到期债务比率(%)
    f165 AS tangible_net_worth_to_debt_ratio,      -- 有形资产净值债务率(%)
    f166 AS equity_multiplier,                     -- 权益乘数(%)
    f167 AS equity_to_total_liabilities_ratio,     -- 股东的权益/负债合计(%)
    f168 AS tangible_assets_to_total_liabilities_ratio, -- 有形资产/负债合计(%)
    f169 AS ocf_to_total_liabilities_ratio,        -- 经营活动现金流净额/负债合计(%)
    f170 AS ebitda_to_total_liabilities_ratio,     -- EBITDA/负债合计(%)

    ----------------- 运营效率 -----------------
    f171 AS ar_turnover,                           -- 应收账款周转率
    f172 AS inventory_turnover,                    -- 存货周转率
    f173 AS working_capital_turnover,              -- 营运资金周转率
    f174 AS total_asset_turnover,                  -- 总资产周转率
    f175 AS fixed_asset_turnover,                  -- 固定资产周转率
    f176 AS ar_turnover_days,                      -- 应收账款周转天数
    f177 AS inventory_turnover_days,               -- 存货周转天数
    f178 AS current_asset_turnover,                -- 流动资产周转率
    f179 AS current_asset_turnover_days,           -- 流动资产周转天数
    f180 AS total_asset_turnover_days,             -- 总资产周转天数
    f181 AS equity_turnover,                       -- 股东权益周转率

    ----------------- 成长能力 -----------------
    f182 AS revenue_growth_rate,                   -- 营业收入增长率(%)
    f183 AS net_profit_growth_rate,                -- 净利润增长率(%)
    f184 AS net_assets_growth_rate,                -- 净资产增长率(%)
    f185 AS fixed_assets_growth_rate,              -- 固定资产增长率(%)
    f186 AS total_assets_growth_rate,              -- 总资产增长率(%)
    f187 AS investment_income_growth_rate,         -- 投资收益增长率(%)
    f188 AS operating_profit_growth_rate,          -- 营业利润增长率(%)
    f189 AS eps_excl_extra_yoy,                    -- 扣非每股收益同比(%)
    f190 AS net_profit_excl_extra_yoy,             -- 扣非净利润同比(%)

    ----------------- 盈利能力 -----------------
    f192 AS cost_expense_profit_ratio,             -- 成本费用利润率(%)
    f193 AS operating_margin,                      -- 营业利润率
    f194 AS business_taxes_rate,                   -- 营业税金率
    f195 AS operating_cost_ratio,                  -- 营业成本率
    f196 AS roe_alt,                               -- 净资产收益率（另一口径）
    f197 AS investment_return_ratio,               -- 投资收益率
    f198 AS net_margin,                            -- 销售净利率(%)
    f199 AS roa,                                   -- 总资产净利率(ROA)
    f200 AS net_profit_margin,                     -- 净利润率
    f201 AS gross_margin,                          -- 销售毛利率(%)
    f202 AS three_expenses_ratio,                  -- 三费比重
    f203 AS admin_expense_ratio,                   -- 管理费用率
    f204 AS finance_expense_ratio,                 -- 财务费用率
    f205 AS net_profit_excl_extra,                 -- 扣除非经常性损益后的净利润

    -- EBIT / EBITDA
    f206 AS ebit,                                  -- 息税前利润
    f207 AS ebitda,                                -- 息税折旧摊销前利润
    f208 AS ebitda_margin,                         -- EBITDA/营业总收入(%)

    ----------------- 资本结构 -----------------
    f209 AS debt_to_asset_ratio,                   -- 资产负债率(%)
    f210 AS current_assets_ratio,                  -- 流动资产比率
    f211 AS monetary_funds_ratio,                  -- 货币资金比率
    f212 AS inventory_ratio,                       -- 存货比率
    f213 AS fixed_assets_ratio,                    -- 固定资产比率
    f214 AS liability_structure_ratio,             -- 负债结构比
    f215 AS equity_to_total_invested_capital_ratio,-- 归母权益/全部投入资本(%)
    f216 AS equity_to_interest_bearing_debt_ratio, -- 股东权益/带息债务(%)
    f217 AS tangible_assets_to_net_debt_ratio,     -- 有形资产/净债务(%)

    ----------------- 综合回报 / TTM -----------------
    f280 AS roe_weighted,                          -- 加权净资产收益率
    f318 AS revenue_ttm,                           -- 营业总收入TTM(万元)
    f328 AS roic,                                  -- 投入资本回报率(ROIC)
    f336 AS dividend_payout_ratio                  -- 股利支付率(%)
FROM raw_caiwu;


-- =====================================
-- 3. 现金流结构与质量 聚焦于“现金流 vs 利润 vs 收入”
-- =====================================
CREATE OR REPLACE VIEW v_cw_cashflow_structure AS
SELECT
    code,
    mkt,
    rdate,
    rpt_type,

    ----------------- 现金流 vs 利润 / 收入 -----------------
    f218 AS operating_cf_per_share,                       -- 每股经营性现金流(元)
    f219 AS cash_content_of_revenue,                      -- 营业收入现金含量(%)
    f220 AS ocf_to_operating_profit_ratio,                -- 经营现金净额/经营净收益(%)
    f221 AS cash_received_to_revenue_ratio,               -- 销售现金/营业收入(%)
    f222 AS ocf_to_revenue_ratio,                         -- 经营现金净额/营业收入
    f223 AS capex_to_depreciation_ratio,                  -- 资本支出/折旧和摊销
    f224 AS net_cash_flow_per_share,                      -- 每股现金流量净额(元)
    f225 AS operating_cf_to_short_term_debt_ratio,        -- 经营净现金比率（短期债务）
    f226 AS operating_cf_to_total_debt_ratio,             -- 经营净现金比率（全部债务）
    f227 AS ocf_to_net_profit_ratio,                      -- 经营现金净流量/净利润
    f228 AS cash_return_on_total_assets,                  -- 全部资产现金回收率

    ----------------- 现金流量表新增通用项目 -----------------
    f560 AS other_cash_effects2,                          -- 其他原因对现金的影响2
    f576 AS cash_received_from_minority_investments_in_subsidiaries, -- 子公司吸收少数股东投资收到的现金
    f577 AS dividends_paid_to_minority_shareholders,      -- 子公司支付给少数股东的股利、利润
    f578 AS depreciation_amortisation_of_investment_properties, -- 投资性房地产折旧及摊销
    f579 AS credit_impairment_losses_cf,                  -- 信用减值损失（现金流相关）
    f580 AS depreciation_of_right_of_use_assets           -- 使用权资产折旧
FROM raw_caiwu;


-- =====================================
-- 4. 股本结构 & 股东/机构持股
-- =====================================
CREATE OR REPLACE VIEW v_cw_holding_structure AS
SELECT
    code,
    mkt,
    rdate,
    rpt_type,

    ----------------- 股本结构 -----------------
    f237 AS total_shares,                           -- 总股本
    f238 AS float_a_shares,                         -- 已上市流通A股
    f239 AS float_b_shares,                         -- 已上市流通B股
    f240 AS float_h_shares,                         -- 已上市流通H股
    f241 AS number_of_shareholders,                 -- 股东人数(户)
    f242 AS shares_held_by_largest_shareholder,     -- 第一大股东持股数量
    f243 AS shares_held_by_top10_float_shareholders,-- 十大流通股东持股合计
    f244 AS shares_held_by_top10_shareholders,      -- 十大股东持股合计

    f263 AS a_shares_held_by_top10_float_shareholders, -- 十大流通股东中A股合计
    f264 AS shares_held_by_largest_float_shareholder,  -- 第一大流通股东持股量
    f265 AS free_float_shares,                      -- 自由流通股
    f266 AS restricted_float_a_shares,              -- 受限流通A股
    f319 AS number_of_employees,                    -- 员工总数(人)

    ----------------- 机构持股（通用） -----------------
    f245 AS total_institutions,                     -- 机构总量(家)
    f246 AS total_institutional_shares,             -- 机构持股总量(股)
    f247 AS qfii_institutions,                      -- QFII机构数
    f248 AS qfii_shares,                            -- QFII持股量
    f249 AS securities_firm_institutions,           -- 券商机构数
    f250 AS securities_firm_shares,                 -- 券商持股量
    f251 AS insurance_institutions,                 -- 保险机构数
    f252 AS insurance_shares,                       -- 保险持股量
    f253 AS fund_institutions,                      -- 基金机构数
    f254 AS fund_shares,                            -- 基金持股量
    f255 AS social_security_institutions,           -- 社保机构数
    f256 AS social_security_shares,                 -- 社保持股量
    f257 AS private_equity_institutions,            -- 私募机构数
    f258 AS private_equity_shares,                  -- 私募持股量
    f259 AS finance_company_institutions,           -- 财务公司机构数
    f260 AS finance_company_shares,                 -- 财务公司持股量
    f261 AS pension_institutions,                   -- 年金机构数
    f262 AS pension_shares,                         -- 年金持股量

    ----------------- 机构持股（细分类型） -----------------
    f271 AS bank_institutions,                      -- 银行机构数
    f272 AS bank_shares,                            -- 银行持股量
    f273 AS general_corporate_institutions,         -- 一般法人机构数
    f274 AS general_corporate_shares,               -- 一般法人持股量
    f276 AS trust_institutions,                     -- 信托机构数
    f277 AS trust_shares,                           -- 信托持股量
    f278 AS special_corporate_institutions,         -- 特殊法人机构数
    f279 AS special_corporate_shares,               -- 特殊法人持股量

    ----------------- 国家队 / 北上资金 -----------------
    f283 AS state_team_shares,                      -- 国家队持股数量（万股）
    f324 AS northbound_institutions,                -- 北上资金机构数
    f325 AS northbound_shares                       -- 北上资金持股量
FROM raw_caiwu;


-- =====================================
-- 5. 预告 / 快报 / 公告事件
-- =====================================
CREATE OR REPLACE VIEW v_cw_event_forecast AS
SELECT
    code,
    mkt,
    rdate,
    rpt_type,

    ----------------- 业绩预告（同比增幅） -----------------
    f284 AS guidance_net_profit_yoy_low,    -- 本期净利润同比增幅下限(%)
    f285 AS guidance_net_profit_yoy_high,   -- 本期净利润同比增幅上限(%)

    ----------------- 业绩快报（核心数据） -----------------
    f286 AS flash_net_profit_attributable_to_owners, -- 归母净利润（业绩快报）
    f287 AS flash_net_profit_excl_extra,             -- 扣非净利润（业绩快报）
    f288 AS flash_total_assets,                      -- 总资产（业绩快报）
    f289 AS flash_net_assets,                        -- 净资产（业绩快报）
    f290 AS flash_eps,                               -- 每股收益（业绩快报）
    f291 AS flash_roe_diluted,                       -- 摊薄净资产收益率（业绩快报）
    f292 AS flash_roe_weighted,                      -- 加权净资产收益率（业绩快报）
    f293 AS flash_net_assets_ps,                     -- 每股净资产（业绩快报）

    ----------------- 公告日期 -----------------
    f312 AS guidance_announcement_date,              -- 业绩预告公告日期
    f313 AS report_announcement_date,                -- 财报公告日期
    f314 AS flash_report_announcement_date,          -- 业绩快报公告日期

    ----------------- 业绩预告（绝对值区间） -----------------
    f316 AS guidance_net_profit_low,                 -- 本期净利润下限(万元)
    f317 AS guidance_net_profit_high,                -- 本期净利润上限(万元)

    ----------------- 业绩快报（分项对比） -----------------
    f329 AS flash_revenue_current,                   -- 快报-营业收入（本期）
    f330 AS flash_revenue_prior,                     -- 快报-营业收入（上期）
    f331 AS flash_operating_profit_current,          -- 快报-营业利润（本期）
    f332 AS flash_operating_profit_prior,            -- 快报-营业利润（上期）
    f333 AS flash_total_profit_current,              -- 快报-利润总额（本期）
    f334 AS flash_total_profit_prior,                -- 快报-利润总额（上期）

    ----------------- 审计意见 -----------------
    f335 AS audit_opinion_code                       -- 审计意见
FROM raw_caiwu;


-- =====================================
-- 6. 金融/保险/券商行业专属扩展
-- =====================================
CREATE OR REPLACE VIEW v_cw_industry_ext AS
SELECT
    code,
    mkt,
    rdate,
    rpt_type,

    ----------------- 资产负债表扩展（金融类资产） -----------------
    f401 AS settlement_reserve,                          -- 结算备付金
    f402 AS funds_lent,                                  -- 拆出资金
    f403 AS loans_and_advances_current,                  -- 发放贷款及垫款(流动资产)
    f404 AS derivative_financial_assets,                 -- 衍生金融资产
    f405 AS premiums_receivable,                         -- 应收保费
    f406 AS reinsurance_receivables,                     -- 应收分保账款
    f407 AS receivables_from_reinsurers_on_reserves,     -- 应收分保合同准备金
    f408 AS financial_assets_purchased_under_resale_agreements, -- 买入返售金融资产
    f409 AS assets_held_for_sale,                        -- 划分为持有待售的资产
    f410 AS loans_and_advances_noncurrent,               -- 发放贷款及垫款(非流动资产)

    ----------------- 负债端扩展（金融类负债） -----------------
    f411 AS borrowings_from_central_bank,                -- 向中央银行借款
    f412 AS deposits_from_customers_and_banks,           -- 吸收存款及同业存放
    f413 AS funds_borrowed,                              -- 拆入资金
    f414 AS derivative_financial_liabilities,            -- 衍生金融负债
    f415 AS financial_assets_sold_under_repurchases,     -- 卖出回购金融资产款
    f416 AS fees_and_commissions_payable,                -- 应付手续费及佣金
    f417 AS reinsurance_payables,                        -- 应付分保账款
    f418 AS insurance_contract_reserves,                 -- 保险合同准备金
    f419 AS agency_trading_securities_funds,             -- 代理买卖证券款
    f420 AS agency_underwriting_securities_funds,        -- 代理承销证券款
    f421 AS liabilities_held_for_sale,                   -- 划分为持有待售的负债

    ----------------- 其他负债与权益工具 -----------------
    f422 AS provisions_ext,                              -- 预计负债
    f423 AS deferred_income_current,                     -- 递延收益（流动负债）
    f424 AS preference_shares_liability,                 -- 优先股（负债）
    f425 AS perpetual_bonds_liability,                   -- 永续债（负债）
    f426 AS long_term_employee_benefits_payable,         -- 长期应付职工薪酬
    f427 AS preference_shares_equity,                    -- 优先股（权益）
    f428 AS perpetual_bonds_equity,                      -- 永续债（权益）

    ----------------- 金融资产投资 -----------------
    f429 AS debt_investments,                            -- 债权投资
    f430 AS other_debt_investments,                      -- 其他债权投资
    f431 AS other_equity_investments,                    -- 其他权益工具投资
    f432 AS other_noncurrent_financial_assets,           -- 其他非流动金融资产

    ----------------- 合同/应收/其他资产 -----------------
    f433 AS contract_liabilities,                        -- 合同负债
    f434 AS contract_assets,                             -- 合同资产
    f435 AS other_assets_ext,                            -- 其他资产
    f436 AS receivables_financing,                       -- 应收款项融资
    f437 AS right_of_use_assets,                         -- 使用权资产
    f438 AS lease_liabilities,                           -- 租赁负债
    f439 AS loans_and_advances,                          -- 发放贷款及垫款
    f440 AS accounts_receivable_ext,                     -- 应收款项
    f441 AS guarantee_deposits_paid,                     -- 存出保证金

    ----------------- 利润表扩展（金融/保险/券商） -----------------
    f505 AS interest_income,                             -- 利息收入
    f506 AS earned_premiums,                             -- 已赚保费
    f507 AS fee_and_commission_income,                   -- 手续费及佣金收入
    f508 AS interest_expense,                            -- 利息支出
    f509 AS fee_and_commission_expense,                  -- 手续费及佣金支出
    f510 AS surrender_payments,                          -- 退保金
    f511 AS net_claims_paid,                             -- 赔付支出净额
    f512 AS net_change_in_insurance_contract_reserves,   -- 提取保险合同准备金净额
    f513 AS policy_dividends_expense,                    -- 保单红利支出
    f514 AS reinsurance_expense,                         -- 分保费用

    ----------------- 现金流量表扩展（金融/保险） -----------------
    f561 AS net_increase_in_customer_and_bank_deposits,  -- 客户存款和同业存放款项净增加额
    f562 AS net_increase_in_borrowings_from_central_bank,-- 向中央银行借款净增加额
    f563 AS net_increase_in_borrowings_from_fis,         -- 向其他金融机构拆入资金净增加额
    f564 AS cash_received_from_original_insurance_premiums, -- 收到原保险合同保费取得的现金
    f565 AS net_cash_received_from_reinsurance,          -- 收到再保险业务现金净额
    f566 AS net_increase_in_policyholder_deposits_and_investments, -- 保户储金及投资款净增加额
    f567 AS net_increase_from_fv_pl_financial_assets_disposal,     -- 处置以公允价值计量且变动计入当期损益金融资产净增加额
    f568 AS cash_received_from_interest_fees_commissions,          -- 收取利息、手续费及佣金的现金
    f569 AS net_increase_in_funds_borrowed,              -- 拆入资金净增加额
    f570 AS net_increase_in_repo_business_funds,         -- 回购业务资金净增加额
    f571 AS net_increase_in_loans_and_advances,          -- 客户贷款及垫款净增加额
    f572 AS net_increase_in_deposits_with_cb_and_banks,  -- 存放中央银行和同业款项净增加额
    f573 AS cash_paid_for_original_insurance_claims,     -- 支付原保险合同赔付款项的现金
    f574 AS cash_paid_for_interest_fees_commissions,     -- 支付利息、手续费及佣金的现金
    f575 AS cash_paid_for_policy_dividends               -- 支付保单红利的现金
FROM raw_caiwu;


-- =====================================
-- 7. 财务因子库
-- 规模 & 每股| EPS、扣非 EPS、每股净资产 + 资产/负债/权益总量 → 基本 size + “每股口径”。
-- 盈利能力   | ROE/ROA/ROIC + 毛利率、净利率、EBITDA Margin，一套完整利润结构。
-- 增长      | 收入/净利/总资产/营业利润 + 扣非 EPS/净利的同比增速，兼顾规模和质量。
-- 杠杆 & 偿债| 传统三大比率 + 资产负债率 + OCF/负债、EBITDA/负债、有息负债率 → 看“扛周期能力”。
-- 效率      | 应收、存货、总资产、权益周转率+天数，方便你做营运资本和运营效率的因子。
-- 现金流    | 既有 OCF/收入/净利等比率，又有 TTM 级别的绝对值 + FCF per share，配合利润因子做“盈余质量”。
-- 股本 & 持股结构| 总股本、自由流通、股东户数、机构/北上持股，用来玩流动性、筹码集中度、国家队/北上因子。
-- 质量          | 审计意见 + 股利支付率，这俩对“垃圾过滤器”很实用。
-- =====================================
CREATE OR REPLACE VIEW v_cw_factor_input AS
SELECT
    code,
    mkt,
    rdate,
    rpt_type,

    -- ===== 1. 核心规模 & 每股类 =====
    f0   AS eps_basic,                         -- 基本每股收益
    f1   AS eps_excl_extra,                    -- 扣非每股收益
    f3   AS nav_ps,                            -- 每股净资产
    f5   AS roe_basic,                         -- 净资产收益率（每股指标口径）
    f73  AS operating_revenue,                 -- 营业收入
    f85  AS operating_profit,                  -- 营业利润
    f91  AS total_profit,                      -- 利润总额
    f95  AS net_profit_parent,                 -- 归母净利润
    f205 AS net_profit_excl_extra,             -- 扣非归母净利润

    f20  AS total_current_assets,              -- 流动资产合计
    f26  AS fixed_assets,                      -- 固定资产
    f39  AS total_assets,                      -- 资产总计
    f53  AS total_current_liabilities,         -- 流动负债合计
    f62  AS total_liabilities,                 -- 负债合计
    f71  AS total_equity,                      -- 所有者权益合计
    f270 AS equity_attributable_to_owners,     -- 归母股东权益（资产负债表）

    -- ===== 2. 盈利能力 / 回报类 =====
    f193 AS operating_margin_pct,              -- 营业利润率(%)
    f196 AS roe,                               -- 净资产收益率(通常口径)
    f197 AS investment_return_pct,             -- 投资收益率(%)
    f198 AS sales_net_margin_pct,              -- 销售净利率(%)
    f199 AS roa,                               -- 总资产净利率
    f200 AS net_profit_margin_pct,             -- 净利润率(%)
    f201 AS gross_margin_pct,                  -- 销售毛利率(%)
    f208 AS ebitda_margin_pct,                 -- EBITDA 利润率(%)
    f280 AS roe_weighted,                      -- 加权 ROE
    f328 AS roic,                              -- ROIC 投入资本回报率

    -- ===== 3. 增长能力 =====
    f182 AS revenue_growth_yoy_pct,            -- 营业收入同比增速(%)
    f183 AS net_profit_growth_yoy_pct,         -- 净利润同比增速(%)
    f186 AS total_assets_growth_yoy_pct,       -- 总资产同比增速(%)
    f188 AS operating_profit_growth_yoy_pct,   -- 营业利润同比增速(%)
    f189 AS eps_excl_extra_growth_yoy_pct,     -- 扣非 EPS 同比(%)
    f190 AS net_profit_excl_extra_growth_yoy_pct, -- 扣非净利润同比(%)

    -- ===== 4. 杠杆 / 偿债能力 =====
    f158 AS current_ratio,                     -- 流动比率
    f159 AS quick_ratio,                       -- 速动比率
    f160 AS cash_ratio_pct,                    -- 现金比率(%)
    f161 AS interest_coverage,                 -- 利息保障倍数
    f209 AS debt_to_asset_pct,                 -- 资产负债率(%)

    f166 AS equity_multiplier_pct,             -- 权益乘数(%)
    f169 AS ocf_to_total_liabilities_pct,      -- 经营现金流 / 负债合计(%)
    f170 AS ebitda_to_total_liabilities_pct,   -- EBITDA / 负债合计(%)
    f165 AS tangible_net_worth_to_debt_pct,    -- 有形净资产 / 债务(%)
    f326 AS interest_bearing_debt_ratio,       -- 有息负债率

    -- ===== 5. 经营效率 =====
    f171 AS ar_turnover,                       -- 应收账款周转率
    f172 AS inventory_turnover,                -- 存货周转率
    f174 AS asset_turnover,                    -- 总资产周转率
    f176 AS ar_turnover_days,                  -- 应收账款周转天数
    f177 AS inventory_turnover_days,           -- 存货周转天数
    f181 AS equity_turnover,                   -- 股东权益周转率

    -- ===== 6. 现金流比率 =====
    f218 AS ocf_per_share,                     -- 每股经营性现金流
    f219 AS cash_content_of_revenue_pct,       -- 收入现金含量(%)
    f220 AS ocf_to_operating_profit_pct,       -- 经营现金流 / 经营收益(%)
    f221 AS cash_sales_to_revenue_pct,         -- 现金售货 / 营业收入(%)
    f222 AS ocf_to_revenue,                    -- 经营现金流 / 营业收入
    f223 AS capex_to_depr,                     -- 资本支出 / 折旧摊销
    f224 AS net_cf_per_share,                  -- 每股现金流量净额
    f225 AS ocf_to_short_term_debt,            -- 经营净现金比率(短期债务)
    f226 AS ocf_to_total_debt,                 -- 经营净现金比率(全部债务)
    f227 AS ocf_to_net_profit,                 -- 经营现金流 / 净利润
    f228 AS cash_return_on_assets,             -- 资产现金回收率

    -- ===== 7. 现金流绝对额 / TTM =====
    f106 AS net_cash_from_operating_activities,   -- 经营活动现金流净额
    f118 AS net_cash_from_investing_activities,   -- 投资活动现金流净额
    f127 AS net_cash_from_financing_activities,   -- 筹资活动现金流净额
    f130 AS net_increase_in_cash_and_equivalents, -- 现金及等价物净增加额

    f306 AS ocf_last_12m,                      -- 近一年经营活动现金流净额
    f309 AS net_cash_flow_last_12m,           -- 近一年现金净流量
    f315 AS investing_cf_last_12m,            -- 近一年投资活动现金流净额
    f320 AS free_cf_to_firm_ps,               -- 每股企业自由现金流
    f321 AS free_cf_to_equity_ps,             -- 每股股东自由现金流

    -- ===== 8. 股本结构 & 机构 / 北上 =====
    f237 AS total_shares,                     -- 总股本
    f238 AS float_a_shares,                   -- 已上市流通 A 股
    f265 AS free_float_shares,                -- 自由流通股
    f241 AS shareholder_count,                -- 股东户数
    f242 AS largest_shareholder_shares,       -- 第一大股东持股
    f243 AS top10_float_shareholders_shares,  -- 十大流通股东持股合计

    f245 AS institution_count,                -- 机构数
    f246 AS institutional_shares,             -- 机构持股量
    f324 AS northbound_institution_count,     -- 北上资金机构数
    f325 AS northbound_shares,                -- 北上资金持股量

    -- ===== 9. 质量类 =====
    f335 AS audit_opinion_code,               -- 审计意见(0-未审计,1-无保留,…)
    f336 AS dividend_payout_ratio_pct         -- 股利支付率(%)
FROM raw_caiwu;

