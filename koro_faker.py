from faker import Faker
import pandas as pd
import datetime
import random

# only possible in my env (you have to install sqlite3 in the repo)
import sqlite3

random.seed(10)


def random_users(no_users):
    fake = Faker()
    Faker.seed(0)
    # generate user data according to structure
    rand_user = (
        pd.DataFrame(
            [
                {
                    "firstname": fake.first_name(),
                    "lastname": fake.last_name(),
                    "email": fake.email(),
                    "firstlogin": fake.date_between(datetime.date(2020, 1, 1)),
                }
                for _ in range(no_users)
            ]
        )
        .reset_index()
        .rename({"index": "id"}, axis=1)
    )
    return users_with_accounts_dupilcation(rand_user)


def users_with_accounts_dupilcation(rand_df):
    fake = Faker()
    # obtain sample & keep name & email
    rand_sample = rand_df.sample(
        random.randint(0, len(rand_df) // 10), replace=True, random_state=42
    ).loc[:, ["firstname", "lastname", "email"]]

    # generate date of login
    rand_sample["firstlogin"] = [
        fake.date_between(datetime.date(2020, 1, 1)) for i in range(len(rand_sample))
    ]

    # append and sort according to first login
    rand_sample = rand_df.append(rand_sample).sort_values(by="firstlogin")

    # adjust the id accordingly
    rand_sample["id"] = list(range(len(rand_sample)))
    rand_sample.reset_index(drop=True, inplace=True)
    return rand_sample[["id", "firstname", "lastname", "email", "firstlogin"]]


def create_order(userid, ordertime):
    fake = Faker()
    """returns a dict containing one order
    this might have been a funny recursive task but I wont do it here as i want to finish"""

    u_order = {
        "userid": userid,
        "invoice_amount": round(random.uniform(30, 150), 2),
        "ordertime": fake.date_between(ordertime),
        "orderstatus": random.randint(0, 1),
    }
    return u_order


def order_times_customer(df):
    fake = Faker()
    Faker.seed(0)
    """ I do ignore the last logintime here as it is not really relevant tbh

    I could have modualized this function further but it should be straigt forward """

    # obtain no_account_customer & account_customer for the order generation
    acc_count = (
        df.groupby(by="email")
        .agg({"email": "count"})
        .rename({"email": "count"}, axis=1)
        .sort_values(by="count")
        .reset_index()
    )

    df_orders = pd.DataFrame(
        columns=["userid", "invoice_amount", "ordertime", "orderstatus"]
    )
    # for acc_customers call the create_order function randomly between 0 & 5 times and create as many orders
    # for non_acc_customers call the create_order function as often as they ordered

    for _, u_r in acc_count.iterrows():

        if u_r[1] == 1:
            # giving chance of customer not being in the orders
            for i in range(0, random.randint(0, 5)):
                useridinput_id = df[df.email == u_r[0]]["id"].values[0]
                useridinput_date = pd.to_datetime(
                    df[df.email == u_r[0]]["firstlogin"].values[0]
                )
                df_orders = df_orders.append(
                    create_order(useridinput_id, useridinput_date), ignore_index=True
                )

        else:
            for useridinput in df[df.email == u_r[0]]["id"].values:
                useridinput_date = pd.to_datetime(
                    df[df.id == useridinput]["firstlogin"].values[0]
                )
                df_orders = df_orders.append(
                    create_order(useridinput, useridinput_date), ignore_index=True
                )

    return df_orders.reset_index().rename(columns={"index": "id"})


df_random_users = random_users(500)

df_random_orders = order_times_customer(df_random_users)




# load the data into the database

df_random_orders.drop(columns="order_period", inplace=True)

# Create a SQL connection to our SQLite database
con = sqlite3.connect("/home/angelo/repos/KORO_application_files/korotest.db")
cur = con.cursor()

# Write the new DataFrame to a new SQLite table
df_random_users.to_sql("users", con, if_exists="replace", index=False)
df_random_orders.to_sql("orders", con, if_exists="replace", index=False)
