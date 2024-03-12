Q1

(1)
select corr(total_sales,total_quantity) as correlation
from (
select STOCKCODE, sum(price*quantity) as total_sales , sum(quantity) as total_quantity 
from TABLERETAIL
group by STOCKCODE) s1
;

(2)
SELECT day, totalsales
FROM (
    SELECT DISTINCT 
        TO_CHAR(invoicedate, 'day') AS day, 
        SUM(QUANTITY * PRICE) OVER (PARTITION BY TO_CHAR(invoicedate, 'day')) AS totalsales
    FROM 
        TABLERETAIL
    ORDER BY 
        totalsales DESC
);

(3)
WITH CustomerSegmentation AS (
    SELECT 
        CUSTOMER_ID,
        SUM(QUANTITY * PRICE) AS TOTAL_PURCHASE_AMOUNT
    FROM 
        TABLERETAIL
    GROUP BY 
        CUSTOMER_ID
)
SELECT 
    CUSTOMER_ID,
    CASE 
        WHEN TOTAL_PURCHASE_AMOUNT >= 1000 THEN 'High Value'
        WHEN TOTAL_PURCHASE_AMOUNT >= 500 AND TOTAL_PURCHASE_AMOUNT < 1000 THEN 'Mid Value'
        ELSE 'Low Value'
    END AS CUSTOMER_SEGMENT
FROM 
    CustomerSegmentation;

(4)
select STOCKCODE,product_total_sales,Total_Sales_rank_per_stock from (
SELECT 
    STOCKCODE,SUM(QUANTITY * PRICE) as product_total_sales,
    DENSE_RANK() OVER(ORDER BY SUM(QUANTITY * PRICE) DESC) AS Total_Sales_rank_per_stock
FROM 
    TABLERETAIL
    group by STOCKCODE 
)
where  Total_Sales_rank_per_stock <=10;

(5)
select year,total_sales ,round( ((total_sales - lag(total_sales, 1) OVER (ORDER BY Year)) / lag(total_sales, 1) OVER (ORDER BY Year)) * 100,2) AS Sales_annual_increase from (
select distinct to_char(invoicedate,'yyyy') Year, SUM(QUANTITY * PRICE) over(partition by to_char(invoicedate,'yyyy') order by to_char(invoicedate,'yyyy') )  as total_sales
from TABLERETAIL
);



Q2
CREATE TABLE Customer_Category (
    Category_name VARCHAR2(100), -- Adjust the length according to your needs
    R_factor NUMBER, -- i meant R_factor but this is typo
    fm_factor NUMBER
);


---(Q2)

-- Common Table Expression (CTE) to calculate customer segmentation metrics
with s1 as (
    -- Calculate frequency, recency, and monetary value metrics
    select 
        customer_id,
        count(distinct invoice) as Frequency,
        min(round((select max(invoicedate) from TABLERETAIL) - invoicedate)) as Recency,
        SUM(QUANTITY * PRICE) AS Monetary,
        ntile(5) over(order by count(distinct invoice)) as F_factor,
        ntile(5) over(order by min(trunc((select max(invoicedate) from TABLERETAIL) - invoicedate)) desc) as R_factor,
        ntile(5) over(order by SUM(QUANTITY * PRICE)) as M_factor
    from 
        TABLERETAIL
    group by 
        customer_id
    order by 
        Recency desc
), 
-- Subquery to calculate the combined factor (fm_factor) from F_factor and M_factor
s2 as (
    select 
        s1.*,  
        ntile(5) over(order by ((m_factor+F_factor)/2)) as fm_factor 
    from 
        s1
)
-- Main query to join customer segmentation results with predefined categories
select 
    s2.*, 
    COALESCE(cc.category_name, 'Uncategorized') AS category_name
from 
    s2 
left join 
    customer_category cc on s2.R_factor = cc.R_factor and s2.fm_factor = cc.fm_factor
order by 
    customer_id desc;
	
	

(Q3)

(a)
WITH customerconsecutivedays AS (
    -- This CTE calculates whether each calendar date for each customer is consecutive or not
    SELECT 
        cust_id, 
        calendar_dt,
        CASE 
            -- If the previous calendar date for the same customer is one day before the current one, set flag to 0 (consecutive)
            WHEN calendar_dt - LAG(calendar_dt) OVER (PARTITION BY cust_id ORDER BY calendar_dt) = 1 THEN 0
            -- If not consecutive, set flag to 1
            ELSE 1 
        END AS consecutive_flag
    FROM 
        customers
),
Groupss AS (
    -- This CTE assigns a group ID to consecutive days for each customer
    SELECT 
        cust_id,
        calendar_dt,
        SUM(consecutive_flag) OVER (PARTITION BY cust_id ORDER BY calendar_dt) AS group_id
    FROM 
        customerconsecutivedays
),
Consecutive_occurence AS (
    -- This CTE counts consecutive days for each customer
    SELECT 
        cust_id, 
        COUNT(cust_id) AS Consecutive_day
    FROM 
        Groupss
    GROUP BY 
        cust_id, 
        group_id
)
-- This final query selects the maximum consecutive days for each customer
SELECT 
    cust_id, 
    MAX(Consecutive_day) AS max_consecutive_days
FROM 
    Consecutive_occurence 
GROUP BY 
    cust_id;




(b)

WITH 
    -- Calculate total spending for each customer over time
    total_spending AS (
        SELECT 
            cust_id,
            SUM(amt_le) OVER (PARTITION BY cust_id ORDER BY CUSTOMERS.CALENDAR_DT) AS total_spend,
            CALENDAR_DT
        FROM 
            customers
    ),
    -- Determine the first order date for each customer
    First_order_date AS (
        SELECT 
            DISTINCT cust_id,
            FIRST_VALUE(CALENDAR_DT) OVER (PARTITION BY cust_id ORDER BY CALENDAR_DT) AS first_order_date 
        FROM 
            customers
    ),
    -- Calculate the number of days it takes for each customer to reach $250 spending milestone from their first order date
    Final_result AS (
        SELECT DISTINCT
            t.cust_id,
            -- Calculate the number of days from the first order date to the date when total spending exceeded $250
            (FIRST_VALUE(t.CALENDAR_DT) OVER (PARTITION BY t.cust_id ORDER BY t.CALENDAR_DT) - f.first_order_date) AS Way_to_250,
            -- Show the value of sales when customers first exceeded $250
            MIN(t.total_spend) OVER (PARTITION BY t.cust_id ORDER BY t.CALENDAR_DT) AS First_250_Hit
        FROM 
            total_spending t 
        JOIN 
            First_order_date f ON t.cust_id = f.cust_id
        WHERE 
            t.total_spend >= 250
    )
-- Calculate the average number of days across all customers to reach the $250 spending milestone
SELECT 
    ROUND(AVG(Way_to_250), 2) AS avg_no_days 
FROM 
    Final_result;

