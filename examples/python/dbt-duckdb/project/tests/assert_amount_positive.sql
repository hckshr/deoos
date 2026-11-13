select order_id
from {{ ref('stg_orders') }}
where amount < 0
