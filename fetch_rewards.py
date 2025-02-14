import pandas as pd
import json
from sqlalchemy import create_engine, text


# Load the data
def load_users_data():
    with open("./data/users.json", "r") as f:
        raw_data = f.read().strip().splitlines()
    # Parse each JSON object
    data = [json.loads(line) for line in raw_data]
    # Convert to DataFrame
    users_df = pd.json_normalize(data)
    users_df["created_date"] = pd.to_datetime(users_df["createdDate.$date"])
    users_df["last_login"] = pd.to_datetime(users_df["lastLogin.$date"])

    # Rename columns
    users_df = users_df.rename(
        columns={
            "_id.$oid": "user_id",
            "active": "active",
            "role": "role",
            "signUpSource": "sign_up_source",
        }
    )
    # Drop columns
    users_df = (
        users_df.drop(columns=["createdDate.$date", "lastLogin.$date"])
        .drop_duplicates()
        .reset_index(drop=True)
    )
    return users_df


def load_receipts_data():
    with open("./data/receipts.json", "r") as f:
        raw_data = f.read().strip().splitlines()

    # Parse each JSON object
    data = [json.loads(line) for line in raw_data]

    # Convert to DataFrame
    receipts_df = pd.json_normalize(data)
    return receipts_df


def load_brands_data():
    with open("./data/brands.json", "r") as f:
        raw_data = f.read().strip().splitlines()

    # Parse each JSON object
    data = [json.loads(line) for line in raw_data]

    # Convert to DataFrame
    brands_df = pd.json_normalize(data)
    brands_df.drop_duplicates(inplace=True)
    return brands_df


def create_cpg_df(brands_df):
    # creata new dataframe with cpg id and cpg reference
    cpg_df = (
        brands_df[["cpg.$id.$oid", "cpg.$ref"]].drop_duplicates().reset_index(drop=True)
    )
    cpg_df = cpg_df.rename(
        columns={"cpg.$id.$oid": "cpg_id", "cpg.$ref": "cpg_reference"}
    )
    return cpg_df


def create_category_df(brands_df):
    # create new dataframe with category id and category
    category_df = (
        brands_df[["category", "categoryCode", "cpg.$id.$oid"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    category_df = category_df.rename(
        columns={
            "cpg.$id.$oid": "cpg_id",
            "category": "category_name",
            "categoryCode": "category_code",
        }
    )

    return category_df


def clean_brands(brands_df):
    # Rename columns
    brands_df = brands_df.rename(
        columns={
            "_id.$oid": "brand_uuid",
            "barcode": "barcode",
            "brandCode": "brand_code",
            "name": "brand_name",
            "categoryCode": "category_code",
            "topBrand": "top_brand",
        }
    )
    # Drop columns
    brands_df = (
        brands_df.drop(columns=["cpg.$id.$oid", "cpg.$ref", "category"])
        .drop_duplicates()
        .reset_index(drop=True)
    )

    return brands_df


def create_products_table(receipts_df, brands_df):
    products_df = receipts_df[["rewardsReceiptItemList"]].explode(
        "rewardsReceiptItemList"
    )
    products_df = products_df.dropna(subset=["rewardsReceiptItemList"]).reset_index(
        drop=True
    )
    products_df = products_df.rename(columns={"rewardsReceiptItemList": "product"})
    products_df = pd.json_normalize(products_df["product"])
    products_df = products_df.rename(
        columns={
            "barcode": "product_id",
            "description": "description",
            "itemPrice": "product_price",
            "metabriteCampaignId": "metabrite_campaign_id",
            "rewardsGroup": "rewards_group",
            "rewardsProductPartnerId": "rewards_product_partner_id",
            "brandCode": "brand_code",
            "originalMetaBriteItemPrice": "original_metabrite_item_price",
            "originalMetaBriteBarcode": "original_metabrite_barcode",
            "originalMetaBriteDescription": "original_metabrite_description",
            "competitorRewardsGroup": "competitor_rewards_group",
            "competitiveProduct": "competitive_product",
        }
    )
    # merge with brands_df to get brand_id and drop brand_code
    products_df = products_df.merge(brands_df, on="brand_code", how="left")
    # select columns
    products_df = (
        products_df[
            [
                "product_id",
                "brand_uuid",
                "description",
                "metabrite_campaign_id",
                "original_metabrite_barcode",
                "original_metabrite_description",
                "original_metabrite_item_price",
                "rewards_group",
                "rewards_product_partner_id",
                "product_price",
                "competitive_product",
                "competitor_rewards_group",
            ]
        ]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    return products_df


def create_receipts_table(receipts_df):
    receipts_df = receipts_df.rename(
        columns={
            "_id.$oid": "receipt_uuid",
            "bonusPointsEarned": "bonus_points_earned",
            "bonusPointsEarnedReason": "bonus_points_reason",
            "createDate.$date": "create_date",
            "dateScanned.$date": "date_scanned",
            "finishedDate.$date": "finished_date",
            "modifyDate.$date": "modify_date",
            "pointsAwardedDate.$date": "points_awarded_date",
            "pointsEarned": "points_earned",
            "purchaseDate.$date": "purchase_date",
            "purchasedItemCount": "purchased_item_count",
            "rewardsReceiptStatus": "rewards_receipt_status",
            "rewardsReceiptStatusReason": "rewards_receipt_status_reason",
            "totalSpent": "total_spent",
            "userId": "user_id",
        }
    )
    receipts_df["create_date"] = pd.to_datetime(receipts_df["create_date"])
    receipts_df["date_scanned"] = pd.to_datetime(receipts_df["date_scanned"])
    receipts_df["finished_date"] = pd.to_datetime(receipts_df["finished_date"])
    receipts_df["modify_date"] = pd.to_datetime(receipts_df["modify_date"])
    receipts_df["points_awarded_date"] = pd.to_datetime(
        receipts_df["points_awarded_date"]
    )
    receipts_df["purchase_date"] = pd.to_datetime(receipts_df["purchase_date"])
    receipts_df = (
        receipts_df.drop(columns=["rewardsReceiptItemList"])
        .drop_duplicates()
        .reset_index(drop=True)
    )
    # modify the value "FINISHED" to "ACCEPTED" in rewards_receipt_status
    receipts_df["rewards_receipt_status"] = receipts_df["rewards_receipt_status"].apply(
        lambda x: "ACCEPTED" if x == "FINISHED" else x
    )

    return receipts_df


def create_receipt_items_table(receipts_df):
    with open("./data/receipts.json", "r") as f:
        raw_data = f.read().strip()
    # Convert to JSON Lines (each line is a valid JSON object)
    json_objects = [
        json.loads(line)
        for line in raw_data.strip().split("\n")
        if "rewardsReceiptItemList" in line
    ]

    # Normalize the nested `rewardsReceiptItemList`
    receipt_items_df = pd.json_normalize(
        json_objects,
        record_path=["rewardsReceiptItemList"],  # Flatten this field
        meta=["_id"],  # Keep these at the top level
    )
    # replace null barcode with itemNumber and drop itemNumber
    receipt_items_df["barcode"] = receipt_items_df["barcode"].fillna(
        receipt_items_df["itemNumber"]
    )
    receipt_items_df["receipt_id"] = receipt_items_df["_id"].apply(lambda x: x["$oid"])
    # rename all camelCase columns to snake_case
    receipt_items_df = receipt_items_df.rename(
        columns={
            "barcode": "product_id",
            "finalPrice": "final_price",
            "needsFetchReview": "needs_fetch_review",
            "partnerItemId": "partner_item_id",
            "preventTargetGapPoints": "prevent_target_gap_points",
            "quantityPurchased": "quantity_purchased",
            "userFlaggedBarcode": "user_flagged_barcode",
            "userFlaggedNewItem": "user_flagged_new_item",
            "userFlaggedPrice": "user_flagged_price",
            "userFlaggedQuantity": "user_flagged_quantity",
            "needsFetchReviewReason": "needs_fetch_review_reason",
            "pointsNotAwardedReason": "points_not_awarded_reason",
            "userFlaggedDescription": "user_flagged_description",
            "pointsPayerId": "points_payer_id",
            "rewardsGroup": "rewards_group",
            "rewardsProductPartnerId": "rewards_product_partner_id",
            "discountedItemPrice": "discounted_item_price",
            "originalReceiptItemText": "original_receipt_item_text",
            "originalMetaBriteQuantityPurchased": "original_metabrite_quantity_purchased",
            "pointsEarned": "points_earned",
            "targetPrice": "target_price",
            "originalFinalPrice": "original_final_price",
            "priceAfterCoupon": "price_after_coupon",
        }
    )
    # select columns all those renamed earlier
    receipt_items_df = (
        receipt_items_df[
            [
                "receipt_id",
                "product_id",
                "final_price",
                "needs_fetch_review",
                "partner_item_id",
                "prevent_target_gap_points",
                "quantity_purchased",
                "user_flagged_barcode",
                "user_flagged_new_item",
                "user_flagged_price",
                "user_flagged_quantity",
                "needs_fetch_review_reason",
                "points_not_awarded_reason",
                "points_payer_id",
                "rewards_group",
                "user_flagged_description",
                "rewards_product_partner_id",
                "discounted_item_price",
                "original_receipt_item_text",
                "original_metabrite_quantity_purchased",
                "points_earned",
                "target_price",
                "original_final_price",
                "price_after_coupon",
            ]
        ]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    return receipt_items_df


def store_data_in_sql(
    engine,
    users_df,
    receipts_df,
    brands_df,
    products_df,
    cpg_df,
    category_df,
    receipt_items_df,
):

    # Store the DataFrames in the database
    users_df.to_sql("users", engine, if_exists="replace", index=False)
    receipts_df.to_sql("receipts", engine, if_exists="replace", index=False)
    brands_df.to_sql("brands", engine, if_exists="replace", index=False)
    products_df.to_sql("products", engine, if_exists="replace", index=False)
    cpg_df.to_sql("cpg", engine, if_exists="replace", index=False)
    category_df.to_sql("category", engine, if_exists="replace", index=False)
    receipt_items_df.to_sql("receipt_items", engine, if_exists="replace", index=False)


def get_average_spent(engine):
    query = """SELECT avg(total_spent), rewards_receipt_status
                FROM receipts
                where rewards_receipt_status is not null
                and rewards_receipt_status in ('REJECTED', 'ACCEPTED')
                group by rewards_receipt_status
                order by avg(total_spent) desc"""
    with engine.connect() as connection:
        result = connection.execute(text(query))
        return result.fetchall()


def get_total_items_purchased(engine):
    query = """SELECT sum(purchased_item_count), rewards_receipt_status
                FROM receipts
                where rewards_receipt_status is not null
                and rewards_receipt_status in ('REJECTED', 'ACCEPTED')
                group by rewards_receipt_status
                order by sum(purchased_item_count) desc"""
    with engine.connect() as connection:
        result = connection.execute(text(query))
        return result.fetchall()


# What are the top 5 brands by receipts scanned for most recent month?
def get_top_5_brands(engine):
    query = """SELECT brand_name, count(brand_name) as receipts_scanned
                FROM brands
                JOIN products
                ON brands.brand_uuid = products.brand_uuid
                JOIN receipt_items
                ON products.product_id = receipt_items.product_id
                JOIN receipts
                ON receipt_items.receipt_id = receipts.receipt_uuid
                WHERE date_scanned >= date('now', '-1 month')
                GROUP BY brand_name
                ORDER BY receipts_scanned DESC
                LIMIT 5"""
    with engine.connect() as connection:
        result = connection.execute(text(query))
        return result.fetchall()


# How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the ranking for the previous month?
def get_top_5_brands_comparison(engine):
    query = """SELECT brand_name, count(brand_name) as receipts_scanned
                FROM brands
                JOIN products
                ON brands.brand_uuid = products.brand_uuid
                JOIN receipt_items
                ON products.product_id = receipt_items.product_id
                JOIN receipts
                ON receipt_items.receipt_id = receipts.receipt_uuid
                WHERE date_scanned >= date('now', '-1 month', '-1 month')
                GROUP BY brand_name
                ORDER BY receipts_scanned DESC
                LIMIT 5"""
    with engine.connect() as connection:
        result = connection.execute(text(query))
        return result.fetchall()


# Which brand has the most spend among users who were created within the past 6 months?
def get_top_brand_spend(engine):
    query = """SELECT brand_name, sum(total_spent) as total_spend
                FROM brands
                JOIN products
                ON brands.brand_uuid = products.brand_uuid
                JOIN receipt_items
                ON products.product_id = receipt_items.product_id
                JOIN receipts
                ON receipt_items.receipt_id = receipts.receipt_uuid
                JOIN users
                ON receipts.user_id = users.user_id
                WHERE users.created_date >= date('now', '-6 month')
                GROUP BY brand_name
                ORDER BY total_spend DESC
                LIMIT 1"""
    with engine.connect() as connection:
        result = connection.execute(text(query))
        return result.fetchall()


# Which brand has the most transactions among users who were created within the past 6 months?
def get_top_brand_transactions(engine):
    query = """SELECT brand_name, count(receipt_uuid) as total_transactions
                FROM brands
                JOIN products
                ON brands.brand_uuid = products.brand_uuid
                JOIN receipt_items
                ON products.product_id = receipt_items.product_id
                JOIN receipts
                ON receipt_items.receipt_id = receipts.receipt_uuid
                JOIN users
                ON receipts.user_id = users.user_id
                WHERE users.created_date >= date('now', '-6 month')
                GROUP BY brand_name
                ORDER BY total_transactions DESC
                LIMIT 1"""
    with engine.connect() as connection:
        result = connection.execute(text(query))
        return result.fetchall()


def create_tables(brands_df, receipts_df):
    # Create the cpg and category DataFrames
    cpg_df = create_cpg_df(brands_df)
    category_df = create_category_df(brands_df)

    # Clean the brands DataFrame
    brands_df = clean_brands(brands_df)

    # Create the products and receipts DataFrames
    products_df = create_products_table(receipts_df, brands_df)
    receipts_df = create_receipts_table(receipts_df)

    # Create the receipt items DataFrame
    receipt_items_df = create_receipt_items_table(receipts_df)

    return cpg_df, category_df, products_df, receipts_df, receipt_items_df


def main():
    # Load the data
    users_df = load_users_data()
    receipts_df = load_receipts_data()
    brands_df = load_brands_data()

    # create tables
    (cpg_df, category_df, products_df, receipts_df, receipt_items_df) = create_tables(
        brands_df, receipts_df
    )

    # Create a connection to the database
    engine = create_engine("sqlite:///fetch_rewards.db")

    # Store the DataFrames in the database
    store_data_in_sql(
        engine,
        users_df,
        receipts_df,
        brands_df,
        products_df,
        cpg_df,
        category_df,
        receipt_items_df,
    )

    # Get the average spent for each rewards receipt status
    avg_spent = get_average_spent(engine)
    print("Average spent for each rewards receipt status:")
    for row in avg_spent:
        print(row)
    print("Highest average spent receipt status: ", avg_spent[0][1])

    # Get the total items purchased for each rewards receipt status
    total_items_purchased = get_total_items_purchased(engine)
    print("Total items purchased for each rewards receipt status:")
    for row in total_items_purchased:
        print(row)
    print("Highest total items purchased receipt status: ", total_items_purchased[0][1])

    """
    ## THE FOLLOWING 4 QUERIES WILL PRODUCE RESULTS ONLY IF THE DATA QUALITY IS GOOD
    # Get the top 5 brands by receipts scanned for most recent month
    top_5_brands = get_top_5_brands(engine)
    print("Top 5 brands by receipts scanned for most recent month:")
    for row in top_5_brands:
        print(row)

    # Get the top 5 brands by receipts scanned for the previous month
    top_5_brands_comparison = get_top_5_brands_comparison(engine)
    print("Top 5 brands by receipts scanned for the previous month:")
    for row in top_5_brands_comparison:
        print(row)

    # Get the brand with the most spend among users who were created within the past 6 months
    top_brand_spend = get_top_brand_spend(engine)
    print(
        "Brand with the most spend among users who were created within the past 6 months:"
    )
    for row in top_brand_spend:
        print(row)

    # Get the brand with the most transactions among users who were created within the past 6 months
    top_brand_transactions = get_top_brand_transactions(engine)
    print(
        "Brand with the most transactions among users who were created within the past 6 months:"
    )
    for row in top_brand_transactions:
        print(row)
    """

if __name__ == "__main__":
    main()
