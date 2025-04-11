import os
import pyodbc
import polars as pl

from xlsxwriter import Workbook





def get_file_path():
    excel_path = f'{os.getcwd()}'
    file = [i for i in os.listdir(excel_path) if ('Inventory Turnover' in i) and ('DATA' not in i)][0]
    file_path = f'{excel_path}\\{file}'

    print(file_path)

    return file_path

def get_months():
    months = (
        pl.read_excel(
            source = file_path, 
            sheet_name = 'DATES',
            schema_overrides={'KPI Month': pl.Date, 'Active?': pl.Boolean}
        )
        .select('KPI Month', 'Active?')
        .filter(pl.col('KPI Month').is_not_null())
        .sort('KPI Month')
    )

    return months

def get_part_classes():
    part_classes = (
        pl.read_excel(
            source = file_path, 
            sheet_name = "Part Classes"
        )
        .select('ClassID', 'Class Group', 'Description')
        .sort('ClassID')
    )

    return part_classes

def update_part_classes(parts: pl.DataFrame):

    parts = (
        parts
        # update EX part class to BEX if raw bar
        .with_columns(
            pl.when(pl.col('Part Number').str.contains('BEX',literal=True))
            .then(pl.lit('BEX').alias('ClassID'))
            .otherwise(pl.col('ClassID'))
        )
        # update EP part class to BEP if raw bar
        .with_columns(
            pl.when(pl.col('Part Number').str.contains('BEP',literal=True))
            .then(pl.lit('BEP').alias('ClassID'))
            .otherwise(pl.col('ClassID'))
        )
    )
    
    return parts

def get_transactions():

    transactions = (
        pl.read_excel(
            source = file_path, 
            sheet_name = 'ALL Transactions',
            schema_overrides = {'UPDATED Quantity' : pl.Float64}
        )
        .select('KPI Month', 'Part Number', 'ClassID', 'UPDATED Quantity', 'Decimal?')
        .rename({'UPDATED Quantity' : 'Received.Quantity'})
    )

    transactions = update_part_classes(transactions)

    return transactions

def get_inventory():

    inventory = (
        pl.read_excel(
            source = file_path, 
            sheet_name = 'On-Hand Inventory',
            schema_overrides = {'TotalOnHandQty' : pl.Float64}
        )
        .select('END Month', 'START Month', 'Part', 'ClassID', 'TotalOnHandQty', 'Decimal?')
        .rename({'Part' : 'Part Number', 'TotalOnHandQty' : 'Quantity'})
    )

    inventory = update_part_classes(inventory)

    return inventory

def get_parts(transactions: pl.DataFrame, inventory: pl.DataFrame):

    parts = (
        # stack all transactions and inventory entries into one DataFrame
        transactions.select('Part Number', 'ClassID', 'Decimal?')
        .vstack(
            inventory.select('Part Number', 'ClassID', 'Decimal?')
        )
        .unique()
        # set UoM to Variable if at least one entry has a non-whole number quantity
        .group_by('Part Number', 'ClassID').agg(pl.col('Decimal?').max())
        .with_columns(
            pl.when(pl.col('Decimal?') == True)
            .then(pl.lit('Variable'))
            .otherwise(pl.lit('Each'))
            .alias('UoM')
        )
        .drop('Decimal?')
        .sort(['ClassID', 'UoM', 'Part Number'])
    )

    return parts

def populate_KPI_tables(months: pl.DataFrame, part_classes: pl.DataFrame, parts: pl.DataFrame, transactions: pl.DataFrame, inventory: pl.DataFrame):
    
    part_table = (
        # select only active months
        months.filter(pl.col('Active?') == True).select(pl.col('KPI Month'))
        # cross join with part classes to get all potential month-part class combos
        .join(
            other = part_classes,
            how = 'cross'
        )
        # join with parts to get all potential parts
        .join(
            other = parts,
            how = 'left',
            on = 'ClassID'
        )
        # join with transactions to get received in quantity
        .join(
            other = transactions.drop('ClassID', 'Decimal?'),
            how = 'left',
            on = ['KPI Month', 'Part Number']
        )
        # aggregate values by part number
        .group_by(
            ['KPI Month', 'ClassID', 'Class Group', 'Description', 'Part Number','UoM']
        )
        .agg([pl.col('Received.Quantity').sum()])
        # join with inventory on START Month to get begining of month inventory level
        .join(
            other = inventory.drop('ClassID', 'Decimal?', 'END Month'),
            how = 'left',
            left_on = ['KPI Month', 'Part Number'],
            right_on = ['START Month', 'Part Number']
        )
        .rename({'Quantity' : 'Start.Quantity'})
        # join with inventory on END Month to get end of month inventory level
        .join(
            other = inventory.drop('ClassID', 'Decimal?', 'START Month'),
            how = 'left',
            left_on = ['KPI Month', 'Part Number'],
            right_on = ['END Month', 'Part Number']
        )
        .rename({'Quantity' : 'End.Quantity'})
        .sort(['KPI Month', 'Class Group', 'ClassID', 'Part Number'])
    )

    print('part table')
    print(part_table)
    print(part_table.columns)


    class_table = (
        part_table.group_by(
            ['KPI Month', 'ClassID', 'Class Group', 'Description', 'UoM']
        )
        .agg(
            [pl.col('Received.Quantity').sum(),
             pl.col('Start.Quantity').sum(),
             pl.col('End.Quantity').sum()]
        )
        .sort(['KPI Month', 'Class Group', 'ClassID'])
    )
    print('class table')
    print(class_table)
    print(class_table.columns)

    group_table = (
        class_table.group_by(
            ['KPI Month', 'Class Group', 'UoM']
        )
        .agg(
            [pl.col('Received.Quantity').sum(),
             pl.col('Start.Quantity').sum(),
             pl.col('End.Quantity').sum()]
        )
        .sort(['KPI Month', 'Class Group'])
    )
    print('group table')
    print(group_table)
    print(group_table.columns)

    insert_location = len(file_path) - 5
    new_file_path = file_path[:insert_location] + ' CLEANED DATA' + file_path[insert_location:]

    with Workbook(new_file_path) as wb:
        part_table.write_excel(
            workbook = wb,
            worksheet = 'KPI_Parts'
        )

        class_table.write_excel(
            workbook = wb,
            worksheet = 'KPI_Classes'
        )

        group_table.write_excel(
            workbook = wb,
            worksheet = 'KPI_Groups'
        )

    return ''

file_path = get_file_path()

def main():

    months = get_months()

    part_classes = get_part_classes()

    transactions = get_transactions()

    inventory = get_inventory()

    parts = get_parts(transactions, inventory)

    populate_KPI_tables(months, part_classes, parts, transactions, inventory)


### RUN IT ###
if __name__ == '__main__':
    main()