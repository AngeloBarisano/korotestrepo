import pandas as pd


from koro_faker import df_random_orders
from koro_faker import df_random_users

# python analysis

acquired_users = pd.merge(
    left=df_random_users,
    right=df_random_orders,
    left_on="id",
    right_on="userid",
    how="inner",
)

acquired_users = acquired_users[acquired_users.orderstatus == 1]
df_random_orders["ordertime"] = pd.to_datetime(df_random_orders.ordertime)
df_random_orders["order_period"] = df_random_orders.ordertime.dt.to_period("m")
orders = (
    df_random_orders[df_random_orders.orderstatus == 1]
    .groupby(by="order_period")
    .agg({"order_period": "count"})
    .rename(columns={"order_period": "Total orders"})
    .reset_index()
)

acquired_users["ordertime"] = pd.to_datetime(acquired_users.ordertime)
acquired_users["cohort"] = (
    acquired_users.groupby("userid")["ordertime"].transform("min").dt.to_period("m")
)
newcustomers = (
    acquired_users.drop_duplicates(subset=["email", "cohort"])
    .groupby(by="cohort")
    .agg({"cohort": "count"})
    .rename(columns={"cohort": "New customers"})
    .reset_index()
)

python_answer = (
    pd.merge(left=newcustomers, right=orders, left_on="cohort", right_on="order_period")
    .drop("order_period", axis=1)
    .rename(columns={"cohort": "month"})
)