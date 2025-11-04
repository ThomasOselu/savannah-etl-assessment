-- User summary query
SELECT
    user_id,
    first_name,
    total_spent,
    total_items,
    age,
    city
FROM
    `savannah-etl-project.savannah_assessment.user_summary`
ORDER BY
    total_spent DESC
LIMIT 10;

-- Category summary query
SELECT
    category,
    total_sales,
    total_items_sold
FROM
    `savannah-etl-project.savannah_assessment.category_summary`
ORDER BY
    total_sales DESC;

-- Cart details query
SELECT
    cart_id,
    user_id,
    product_id,
    quantity,
    price,
    total_cart_value,
    first_name,
    last_name,
    product_name,
    category,
    brand
FROM
    `savannah-etl-project.savannah_assessment.cart_details`
WHERE
    cart_id = 1;