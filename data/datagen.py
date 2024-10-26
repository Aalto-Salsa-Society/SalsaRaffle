#type: ignore

# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "faker",
#     "numpy",
#     "polars",
#     "xlsxwriter",
# ]
# ///

#run this script with "uv run datagen.py"

from faker import Faker
import polars as pl
from xlsxwriter import Workbook
from numpy.random import choice

fake = Faker()  

MEMBER_FILENAME = "members.xlsx"
RESPONSES_FILENAME = "responses.xlsx"

NUM_MEMBERS = 300
NUM_APPROVED = 270
NUM_REG = 140
CLASS_DIST = [0.3, 0.15, 0.15, 0.1, 0.2, 0.1] #first choice class pref distribution 
ROLE_DIST = [0.6, 0.4] #follower vs leader ratio
SECOND_PREF_DIST = [0.3, 0.7] #Has second pref vs doesn't have second pref ratio
BOTH_PREF_DIST = [0.8, 0.2] #yes to both vs no to both

CLASSES = ["Salsa Level 1", "Salsa Level 2", "Salsa Level 3", "Salsa Level 4","Bachata Level 1", "Bachata Level 2"]
ROLES = ["Leader", "Follower"]
SECOND_PREF_OPTIONS = ["Yes", "No"]

# REGISTRATION_COLUMNS: Final = {
#     "Telegram handle": Col.HANDLE.value,
#     "Full name (first and last name)": Col.NAME.value,
#     "Email address": Col.EMAIL.value,
#     "First preference": Col.P1_CLASS.value,
#     "First preference dance role": Col.P1_ROLE.value,
#     "I have a second preference": Col.HAS_P2.value,
#     "Second preference": Col.P2_CLASS.value,
#     "Second preference dance role": Col.P2_ROLE.value,
#     "Both preferences": Col.ONLY_1.value,
# }

def main(): 
    #main file
    print("this is running datagen")
    df = gen_data()
    write_members_file(df)

    ##create old group (csv) file based on who is an approved member 
    ##create old attendance sheet based on old group csv file
    #create registration sheet, with some returning members, some members from rejected list, and some new members 


def gen_data():
    #generate fake data for membership file
    names = [fake.name() for x in range(NUM_MEMBERS)]
    emails = [name.split(" ")[0] + "." + name.split(" ")[1]  + "@email.com" for name in names]
    handles = ["@" + name.replace(" ", "").lower() for name in names]
    approved = [str(True) for x in range(NUM_APPROVED)] + [str(False) for x in range(NUM_MEMBERS - NUM_APPROVED)]
    class_pref = [str(choice(CLASSES, p=CLASS_DIST)) for x in range(NUM_MEMBERS)]
    role_pref = [str(choice(ROLES, p=ROLE_DIST)) for x in range(NUM_MEMBERS)]
    has_second_pref = [str(choice(SECOND_PREF_OPTIONS, p=SECOND_PREF_DIST)) for x in range(NUM_MEMBERS)]

    df = pl.DataFrame({
        "Full name (first and last name)" : names, 
        "Email address" : emails, 
        "Handle":handles,
        "Approved":approved, 
        "First preference": class_pref, 
        "First preference dance role": role_pref, 
        "I have a second preference" : has_second_pref
    })

    df = gen_second_pref_data(df)
   
    return df

def gen_second_pref_data(df):
    # df = df.select([
    #     pl.all(), 
    #     pl.lit(None).alias("Second preference"), 
    #     pl.lit(None).alias("Second preference dance role"), 
    #     pl.lit(None).alias("Both preferences"), 
    # ])

    # df.with_columns(
    #     pl.when(pl.col("I have a second preference").eq("Yes")).then(pl.lit(choice(ROLES))).alias("Second preference dance role")
    # )
    #filter(pl.col("I have a second preference").eq("Yes"))

    second_class, second_role, both_pref = [], [], []
  
    for row in df.iter_rows(): #there should be a way to replace this for loop with polars functions
        if row[6] == "Yes": 
            classes = CLASSES.copy()
            classes.remove(row[4])
            second_class.append(str(choice(classes))) #randomnly pick a class that isn't the first class, even distribution 
            second_role.append(str(choice(ROLES))) #randomnly pick a role, even distribution
            both_pref.append(str(choice(["True", "False"], p=BOTH_PREF_DIST)))
        else: 
            second_class.append(None)
            second_role.append(None)
            both_pref.append(None)
    
    df = df.select([
        pl.all(), 
        pl.lit(pl.Series(second_class)).alias("Second preference"),
        pl.lit(pl.Series(second_role)).alias("Second preference dance role"),
        pl.lit(pl.Series(both_pref)).alias("Both preference"), 
    ])

    return df

        

def write_members_file(df):
    with Workbook(MEMBER_FILENAME) as wb:
        df.select(["Handle", "Approved"]).write_excel(workbook=wb)
    

def write_registration_file(df):
    #generate fake data for membership file
    # rename handle to Telegram handle
    print("hello there")
    
if __name__ == "__main__":
    main()
