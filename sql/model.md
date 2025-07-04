// --- Dimensions ---

// Customer Dimensions
Table d_customers as customers {
customer_id hugeint [pk]
first_name varchar(128)
last_name varchar(128)
}

Table d_customer_identifiers {
identifier_id hugeint [pk]
customer_id hugeint [ref: > d_customers.customer_id]
identifier_type varchar(50) // e.g., 'CPF', 'SSN'
identifier_value varchar(256)
country_id hugeint [ref: > d_country.country_id]
}

// Location Dimensions (Maintained from original model)
Table d_country as country {
country_id hugeint [pk]
country_name varchar(128)
}

Table d_state as state {
state_id hugeint [pk]
state_name varchar(128)
country_id hugeint [ref: > d_country.country_id]
}

Table d_city as city {
city_id hugeint [pk]
city_name varchar(256)
state_id hugeint [ref: > d_state.state_id]
}

// Time Dimensions (Maintained from original model)
Table d_year {
year_id int [pk]
action_year int
}

Table d_month {
month_id int [pk]
action_month int
}

Table d_week {
week_id int [pk]
action_week int
}

Table d_weekday {
weekday_id int [pk]
action_weekday varchar(128)
}

Table d_time {
time_id int [pk]
full_timestamp timestamp
year_id int [ref: > d_year.year_id]
month_id int [ref: > d_month.month_id]
week_id int [ref: > d_week.week_id]
weekday_id int [ref: > d_weekday.weekday_id]
}

// Product & Transaction Dimensions (New)
Table d_products {
product_id int [pk]
product_name varchar(128) // e.g., 'Credit Card', 'NuInvest Stocks'
product_category varchar(128) // e.g., 'Credit', 'Investment'
}

Table d_transaction_types {
transaction_type_id int [pk]
transaction_type_name varchar(128) // e.g., 'CREDIT_PURCHASE', 'INVESTMENT_BUY'
is_financial boolean // True if it affects balance
}

// --- Facts ---

// Contract Fact (New)
Table f_contracts {
contract_id hugeint [pk]
customer_id hugeint [ref: > d_customers.customer_id]
product_id int [ref: > d_products.product_id]
contract_status varchar(50) // e.g., 'ACTIVE', 'BLOCKED'
start_date timestamp
end_date timestamp
}

// Contract Attributes (New, entity–attribute–value pattern for flexibility)
Table f_contract_attributes {
attribute_id hugeint [pk]
contract_id hugeint [ref: > f_contracts.contract_id]
attribute_name varchar(100) // e.g., 'credit_limit', 'interest_rate'
attribute_value varchar(256)
valid_from timestamp
valid_to timestamp
}

// Transaction Fact (Unified)
Table f_transactions {
transaction_id hugeint [pk]
contract_id hugeint [ref: > f_contracts.contract_id]
transaction_type_id int [ref: > d_transaction_types.transaction_type_id]
requested_at_time_id int [ref: > d_time.time_id]
completed_at_time_id int [ref: > d_time.time_id]
transaction_status varchar(50)
amount decimal(18, 2)
currency varchar(3)
}  