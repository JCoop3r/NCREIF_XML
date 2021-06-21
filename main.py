import os
import pyodbc
import pandas as pd
import numpy as np

def retrieve_data(input_dir: str) -> pd.DataFrame:
    # Change working director to input location
    os.chdir(input_dir)
    """
    # Import NCREIF returns then merge sector and CBSA returns together
    ncreif_returns_cbsa = pd.read_excel('NCreif_Returns.xlsx', sheet_name='CBSA', engine='openpyxl')
    ncreif_returns_sector = pd.read_excel('NCreif_Returns.xlsx', sheet_name='Sector', engine='openpyxl')
    ncreif_returns = pd.concat([ncreif_returns_cbsa, ncreif_returns_sector], axis=0)
    """
    # Import CoStar Factor value data and unpivot it, then merge all sectors together
    costar_factor_data_retail = (
        pd.read_excel('CoStar_FundamentalExtract.xlsx', sheet_name='Retail', engine='openpyxl')
        .melt(id_vars=['Date', 'Segment', 'PropertyType', 'PropertySubType', 'SecurityName'],
              var_name='FieldName', value_name='FieldValue')
    )
    costar_factor_data_office = (
        pd.read_excel('CoStar_FundamentalExtract.xlsx', sheet_name='Office', engine='openpyxl')
        .melt(id_vars=['Date', 'Segment', 'PropertyType', 'PropertySubType', 'SecurityName'],
              var_name='FieldName', value_name='FieldValue')
    )
    costar_factor_data_mf = (
        pd.read_excel('CoStar_FundamentalExtract.xlsx', sheet_name='MF', engine='openpyxl')
        .melt(id_vars=['Date', 'Segment', 'PropertyType', 'PropertySubType', 'SecurityName'],
              var_name='FieldName', value_name='FieldValue')
    )
    costar_factor_data_industrial = (
        pd.read_excel('CoStar_FundamentalExtract.xlsx', sheet_name='Industrial', engine='openpyxl')
        .melt(id_vars=['Date', 'Segment', 'PropertyType', 'PropertySubType', 'SecurityName'],
              var_name='FieldName', value_name='FieldValue')
    )
    costar_factor_data = (
        pd.concat([costar_factor_data_retail, costar_factor_data_office, costar_factor_data_mf,
                   costar_factor_data_industrial], axis=0)
        .assign(Date=lambda x: pd.to_datetime(x.Date).apply(lambda y: y.date()))
    )

    # Import CoStar Factor reference data, then join to Factor value data
    costar_factor_names = pd.read_excel('CoStar_FundamentalExtract.xlsx', sheet_name='Factors', engine='openpyxl')
    costar_factor_data = costar_factor_data.merge(costar_factor_names[['FieldName', 'Style', 'LowHigh']], how='left', on='FieldName')

    # Re-order the columns to match the original SQL download
    cols = ('Date', 'Segment', 'PropertyType', 'PropertySubType', 'FieldName', 'Style', 'LowHigh', 'FieldValue',
            'SecurityName')
    costar_factor_data = (
        costar_factor_data.reindex(columns=cols)
        .dropna(subset=['FieldValue'], axis=0)
        .query(f'Segment == "{segment}"')
        .drop('Segment', axis=1)
    )

    return costar_factor_data



def main(name):
    segment = 'CBSA'
    input_dir = 'C:/Users/asimon1/CBRE, Inc/QIR - Documents/Research and Development/CBRE GI/Factor Analysis/Input'
    output_dir = 'C:/Users/asimon1/CBRE, Inc/QIR - Documents/Research and Development/CBRE GI/Factor Analysis/Output'

    # retrieve raw data from SQL
    # df = retrieve_factor_data(segment=segment)
    df = retrieve_factor_data_excel(segment=segment, input_dir=input_dir)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()