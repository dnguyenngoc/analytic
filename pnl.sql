TRUNCATE TABLE mart_pnl.pnl_accounting_structure;

INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    'V.', 
    'Total Revenue', 
    SUM(total_revenue),
    segment
FROM mart_pnl.mart_pnl
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;

INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    '1.', 
    'Variable Cost', 
    SUM(total_variable_cost),
    segment
FROM mart_pnl.mart_pnl
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;
    
INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    '1.1.', 
    'Personnel Cost', 
    SUM(personnel_cost),
    segment
FROM mart_pnl.mart_pnl
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;

INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    '1.1.1.', 
    'DPOs/IPOs/EPOs', 
    SUM(dpo_ipo_epo),
    segment
FROM mart_pnl.mart_pnl
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;
    
INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    '1.1.1.1.', 
    'Salary (labor contract)', 
    SUM(labor_contract_salary),
    segment
FROM mart_pnl.mart_pnl
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;

INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    '1.1.1.2.', 
    'Salary (Digipay)', 
    SUM(digipay_salary),
    segment
FROM mart_pnl.mart_pnl
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;

INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    '1.1.1.3.', 
    'Training Cost', 
    SUM(training_cost),
    segment
FROM mart_pnl.mart_pnl
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;

INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    '1.1.1.4.', 
    'Salary (Services & Others)', 
    SUM(service_salary),
    segment
FROM mart_pnl.mart_pnl
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;

INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    '1.1.1.5.', 
    'Salary (Probation)', 
    SUM(probation_salary),
    segment
FROM mart_pnl.mart_pnl
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;

INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    '1.1.1.6.', 
    'Overtime expenses', 
    SUM(overtime_expense),
    segment
FROM mart_pnl.mart_pnl
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;

INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    '1.1.1.7.', 
    '3rd shift Allowance', 
    SUM(third_shift_allowance),
    segment
FROM mart_pnl.mart_pnl
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;

INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    '1.1.1.8.', 
    'Severnance Alloowance', 
    SUM(severnance_allowance),
    segment
FROM mart_pnl.mart_pnl
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;
    
INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    '1.1.1.9.', 
    'Other Allowance', 
    SUM(other_allowance),
    segment
FROM mart_pnl.mart_pnl
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;

INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    '1.1.1.10.', 
    '13th Month Salary', 
    SUM(thirteenth_allowance),
    segment
FROM mart_pnl.mart_pnl
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;
    
INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    '1.1.1.11.', 
    'Social Insurance (21.5%)', 
    SUM(social_insurance),
    segment
FROM mart_pnl.mart_pnl
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;

INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    '1.1.1.12.', 
    'Union Fee', 
    SUM(union_fee),
    segment
FROM mart_pnl.mart_pnl
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;

INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    '1.1.1.13.', 
    'Directly Bonus', 
    SUM(directly_bonus),
    segment
FROM mart_pnl.mart_pnl
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;

INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    '1.1.2.', 
    'PJM Salary', 
    SUM(pjm),
    segment
FROM mart_pnl.mart_pnl
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;
    
INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    '1.1.3.', 
    'Software & Tester Salary', 
    SUM(software_tester),
    segment
FROM mart_pnl.mart_pnl
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;

INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    '1.1.4.', 
    'QMD Salary', 
    SUM(qmd),
    segment
FROM mart_pnl.mart_pnl
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;

INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    '1.1.6.', 
    'Cost for non-working time', 
    SUM(non_working_time_cost),
    segment
FROM mart_pnl.mart_pnl
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;

INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    '1.2.', 
    'Employee general expenses', 
    SUM(other_bonus),
    segment
FROM mart_pnl.mart_pnl
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;

INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    '1.3.', 
    'Business Trip', 
    SUM(business_trip),
    segment
FROM mart_pnl.mart_pnl
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;

INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    '1.4.', 
    'Selling and Marketing Expense', 
    SUM(selling_marketing_expenses),
    segment
FROM mart_pnl.mart_pnl
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;

INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    '1.5.', 
    'Outsourcing Cost', 
    SUM(oursourcing),
    segment
FROM mart_pnl.mart_pnl
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;

INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    '1.6.', 
    'Internet line (special project)', 
    SUM(internet_line),
    segment
FROM mart_pnl.mart_pnl
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;

INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    '2.', 
    'Contribution', 
    SUM(contribution),
    segment
FROM mart_pnl.mart_pnl
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;

/*
INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    'PGCR', 
    '% GC/Revenue', 
    SUM(gc_revenue_percent),
    segment
FROM mart_pnl.mart_pnl
WHERE project_name LIKE '%MVL%eClaim'
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;
*/

INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    '3.', 
    'Fixed Cost', 
    SUM(fixed_cost),
    segment
FROM mart_pnl.mart_pnl
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;

INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    '3.1.', 
    'Operating Cost', 
    SUM(operating_cost),
    segment
FROM mart_pnl.mart_pnl
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;

INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    '3.2.', 
    'Depreciation fix asset + tool & small equipments', 
    SUM(depreciation),
    segment
FROM mart_pnl.mart_pnl
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;

INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    '3.3.', 
    'Administration & General expenses (back office)', 
    SUM(administration_general_cost),
    segment
FROM mart_pnl.mart_pnl
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;

INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    'VI.', 
    'EBIT', 
    SUM(ebit),
    segment
FROM mart_pnl.mart_pnl
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;
    
/*
INSERT INTO mart_pnl.pnl_accounting_structure
SELECT 
    month, year, date_key, 
    project_name, 
    customer, 
    'PER', 
    '% EBIT/Rev', 
    CASE SUM(total_revenue)
        WHEN 0 THEN 0::double precision
        ELSE SUM(ebit) / SUM(total_revenue) * 100::double  precision
    END,
    segment
FROM mart_pnl.mart_pnl
WHERE project_name LIKE '%MVL%eClaim'
GROUP BY 
    month, year, date_key, 
    project_name, 
    customer,
    segment;
*/