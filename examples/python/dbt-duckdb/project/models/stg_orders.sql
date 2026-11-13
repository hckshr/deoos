select
    cast(order_id as varchar) as order_id,
    cast(amount as integer) as amount
from {{ ref('raw_orders') }}
