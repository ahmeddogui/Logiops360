import pandas as pd

def transform_supply_chain_data(engine) -> pd.DataFrame:
    query = "SELECT * FROM raw_supply_chain_data"
    df_commandes = pd.read_sql(query, engine)

    commandes_cols = [
        "SKU", "Price", "Availability",
        "Number of products sold", "Revenue generated",
        "Customer demographics", "Stock levels", "Lead times",
        "Order quantities", "Shipping times", "Shipping carriers",
        "Shipping costs", "Supplier name", "Location", "Lead time",
        "Production volumes", "Manufacturing lead time",
        "Manufacturing costs", "Inspection results", "Defect rates",
        "Transportation modes", "Routes", "Costs"
    ]

    df_clean_commandes = df_commandes[commandes_cols].copy()

    df_clean_commandes.columns = [
        col.strip().lower().replace(" ", "_").replace("(", "").replace(")", "")
        for col in df_clean_commandes.columns
    ]

    str_cols = df_clean_commandes.select_dtypes(include='object').columns
    for col in str_cols:
        df_clean_commandes[col] = (
            df_clean_commandes[col]
            .astype(str)
            .str.strip()
            .str.replace(r"\s+", " ", regex=True)
            .str.upper()
        )

    df_clean_commandes = df_clean_commandes.drop_duplicates()

    return df_clean_commandes
