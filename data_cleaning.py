import pandas as pd
import numpy as np 
import re

class DataCleaning:
    """
    A class to clean data.

    ...

    Attributes
    ----------
    self.table : a pandas dataframe object

    Methods
    -------

    remove_nulls : Remove nulls from the whole table
    clean_users : Clean the users table.
    clean_card_data : Clean the card details table
    clean_store_data : Clean the store details table
    clean_products_data : Clean the product details table.
    clean_order_data : Clean the orders table.
    clean_events_data : Clean the events table.
    """
    def __init__(self, df):
        self.table = df
        pass
    
    def remove_nulls(self):
        '''
        Remove nulls from the whole table
        Parameters
        ----------
        none
        
        Returns 
        -------
        self.table
        '''
        self.table.replace('NULL', np.nan, inplace=True)
        self.table.dropna(inplace=True, how='all')
        return self.table
               
    def _validate_countries(self, country_column:str):
        '''
        Check countries are "United Kingdom", "Germany" or "United States".
        
        Parameters
        ----------
        country_column(str) : column name
        
        Returns 
        -------
        self.table
        '''
        valid_countries = ['United Kingdom','Germany', 'United States' ]
        self.table = self.table[self.table[country_column].isin(valid_countries)]
        return self.table
        
    def _validate_names(self, name_column:str):
        '''
        Check the characters in a name column are not numeric.
        
        Parameters
        ----------
        name_column(str) : column name
        
        Returns 
        -------
        self.table
        '''
        digit_match = r'^\d+$'
        first_name_filter = self.table[name_column].str.match(digit_match)
        self.table = self.table[~first_name_filter]
        return self.table
        
    def _validate_emails(self, email_column:str):
        '''
        Replace and double "@@" with a singular one then regex match the email to check its valid.
        
        Parameters
        ----------
        email_column(str) : column name
        
        Returns 
        -------
        self.table
        '''
        self.table[email_column].replace(to_replace = '@@', value = '@',regex=True, inplace=True)
        #Complex email regex pattern found: https://ihateregex.io/expr/email-2/
        email_pattern = r'(([^<>()\[\]\\.,;:\s@"]+(\.[^<>()\[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))'
        filter = self.table[email_column].str.match(email_pattern)
        self.table = self.table[filter]
        return self.table
    
    def _validate_country_code(self, country_code_column:str):
        '''
        Check countries are "UK", "DE" or "US".
        
        Parameters
        ----------
        country_code_column(str) : column name
        
        Returns 
        -------
        self.table
        '''
        self.table[country_code_column].replace(to_replace = 'GG', value = 'G',regex=True, inplace=True)
        valid_country_codes = ['GB','DE', 'US' ]
        self.table = self.table[self.table[country_code_column].isin(valid_country_codes)]
        return self.table
        
    def _validate_address(self,address_column:str):
        '''
        Replace "/n" with a space in address column.
        
        Parameters
        ----------
        address_column(str) : column name
        
        Returns 
        -------
        self.table
        '''
        self.table[address_column] = self.table[address_column].replace({r'\s+$': '', r'^\s+': ''}, regex=True).replace(r'\n',  ' ', regex=True)
        return self.table

    def clean_users(self):
        '''
        Clean the users table.
        Sets the correct data types for the columns of the user data. 
        Validates the first and last name columns.
        Validates the emails.
        Validates the addresses.
        Validates the countries.
        Validates the country codes.
        Removes Nulls.
        
        Parameters
        ----------
        none
        
        Returns 
        -------
        self.table
        '''
        #set correct data types
        self.table.set_index('index', inplace=True)
        self.table['first_name'] = self.table['first_name'].astype('string')
        self.table['last_name'] = self.table['last_name'].astype('string')
        self.table['date_of_birth'] = pd.to_datetime(self.table['date_of_birth'], infer_datetime_format=True, errors='coerce')
        self.table['company'] = self.table['company'].astype('string')
        self.table['email_address'] = self.table['email_address'].astype('string')
        self.table['address'] = self.table['address'].astype('string')
        self.table['country'] = self.table['country'].astype('string')
        self.table['country_code'] = self.table['country_code'].astype('string')
        self.table['phone_number'] = self.table['phone_number'].astype('string')
        self.table['join_date'] = pd.to_datetime(self.table['join_date'], infer_datetime_format=True, errors='coerce')
        
        #validate names
        self._validate_names(name_column='first_name')
        self._validate_names(name_column='last_name')
        
        #validate emails
        self._validate_emails(email_column='email_address')
        
        #validate address
        self._validate_address(address_column='address')
        
        #validate countries
        self._validate_countries(country_column = 'country')
        
        #validate country code
        self._validate_country_code(country_code_column='country_code')
        
        #remove nulls
        self.remove_nulls()
        
        return self.table
    
    def _validate_card_providers(self, card_provider_column : str):
        '''
        Checks the cards in card providers are in the list of top card providers.
        
        Parameters
        ----------
        card_provider_column(str) : column name
        
        Returns 
        -------
        self.table
        '''
        #validate card companies
        valid_card_companies = ['VISA 16 digit','JCB 16 digit','VISA 13 digit','JCB 15 digit','VISA 19 digit','Diners Club / Carte Blanche','American Express','Maestro','Discover','Mastercard']
        self.table = self.table[self.table[card_provider_column].isin(valid_card_companies)]
        return self.table
    
    def _validate_expiry_dates(self, expiry_date_column: str):
        '''
        Checks the expiry dates against a regex.
        
        Parameters
        ----------
        expiry_date_column(str) : column name
        
        Returns 
        -------
        self.table
        '''
        correct_pattern = r'[0-9]{2}(?:\/)[0-9]{2}'
        filter = self.table[expiry_date_column].str.fullmatch(correct_pattern)
        self.table = self.table[filter]
        return self.table
    
    def _validate_card_numbers(self, card_number_column:str):
        '''
        Removes ? from rows
        Drops any row when the card number is not numeric.
        Drops any value longer than 19 chars long
        
        Parameters
        ----------
        card_number_column(str) : column name
        
        Returns 
        -------
        self.table
        '''
        self.table[card_number_column] = self.table[card_number_column].str.replace('?','')
        self.table.drop(self.table[pd.to_numeric(self.table[card_number_column], errors='coerce').notna()].index)
        mask = self.table[card_number_column].str.len() > 19
        self.table = self.table[~mask]
        return self.table
        
    def clean_card_data(self):
        '''
        Clean the card details table
        Sets the correct data types for the card data columns.
        Validates the card providers.
        Validates the expiry dates.
        Validates the card numbers.
        Drops null values.
        
        Parameters
        ----------
        None
        
        Returns 
        -------
        self.table
        '''
        
        self.remove_nulls()
        
        #correct data types
        self.table['card_number'] = self.table['card_number'].astype('string')
        self.table['card_provider'] = self.table['card_provider'].astype('string')
        self.table['expiry_date'] = self.table['expiry_date'].astype('string')
        self.table['date_payment_confirmed'] = pd.to_datetime(self.table['date_payment_confirmed'], errors='coerce')
        
        #validate card numbers
        self._validate_card_numbers(card_number_column='card_number')
        
        #validate card companies
        self._validate_card_providers(card_provider_column='card_provider')
        
        #validate expiry dates
        self._validate_expiry_dates(expiry_date_column='expiry_date')
        
        return self.table
    
    def _validate_continent(self, continent_column:str):
        '''
        Removes instances of "ee" from continent column.
        
        Parameters
        ----------
        continent_column(str) : column name
        
        Returns 
        -------
        self.table'''
        self.table[continent_column].replace(to_replace = 'ee', value = '',regex=True, inplace=True)
        return self.table
    
    def _replace_nulls_if_web(self):
        '''
        Replace nulls in the column where store type contains "Web".
        
        Parameters
        ----------
        None
        
        Returns 
        -------
        self.table
        '''
        web_portal_row = self.table[self.table['store_type'] == 'Web Portal']
        web_portal_row.fillna('N/A', inplace=True)
        return self.table
    
    def _get_digits(self, x): 
        '''
        Simple function to remove all non numerical values and replace them with blank space
        
        Parameters
        ----------
        x : row value
        
        Returns 
        -------
        Substituted value
        '''
        return re.sub(r'\D', '', x)
    
    def _clean_staff_numbers(self, staff_number_columnn:str):
        '''
        Clean the staff numbers by removing any non-numerical values
        
        Parameters
        ----------
        staff_number_column(str) : The name of the column
        
        Returns 
        -------
        self.table
        '''
        self.table[staff_number_columnn] = self.table[staff_number_columnn].astype(str)
        self.table[staff_number_columnn] = self.table[staff_number_columnn].fillna('').astype(str)
        self.table[staff_number_columnn] = self.table[staff_number_columnn].apply(self._get_digits)
        
        return self.table


    
    def clean_store_data(self):
        '''
        Clean the store details table
        Drops the "lat" column. 
        Set correct data types.
        Validate country codes.
        Validate addresses.
        Validate continent.
        Replace nulls in the Web Portal column.
        Remove nulls.
        
        Parameters
        ----------
        None
        
        Returns 
        -------
        self.table
        '''
        #drop the lat column
        self.table = self.table.drop(columns=['lat'])
        #clean staff numbers
        self._clean_staff_numbers('staff_numbers')
        #correct data types
        self.table['address'] = self.table['address'].astype('string')
        self.table['longitude'] = pd.to_numeric(self.table['longitude'], errors='coerce')
        self.table['locality'] = self.table['locality'].astype('string')
        self.table['store_code'] = self.table['store_code'].astype('string')
        self.table['staff_numbers'] = pd.to_numeric(self.table['staff_numbers'], errors='coerce')
        self.table['opening_date'] = pd.to_datetime(self.table['opening_date'], infer_datetime_format=True, errors='coerce')
        self.table['store_type'] = self.table['store_type'].astype('string')
        self.table['latitude'] = pd.to_numeric(self.table['latitude'], errors='coerce')
        self.table['country_code'] = self.table['country_code'].astype('string')
        self.table['continent'] = self.table['continent'].astype('string')
        
        #validata country code
        self._validate_country_code('country_code')
        #clean continent table
        self._validate_continent(continent_column='continent')
        #validate address
        self._validate_address(address_column='address')
        
        #Replace nulls in the Web store row
        self._replace_nulls_if_web()
        #drop null values
        self.remove_nulls()
        return self.table
    
    def _split_weight_units(self, x):
        '''Function to split the weights column into the units and the values.
        Null values are returned as numpy nulls.
        Other values are split accordingly
        
        Parameters
        ----------
        product_price_column(str) : column name
        
        Returns 
        -------
        np.nan, np.nan
        or
        weight , unit 
        '''
        try:
            if pd.isna(x): 
                return np.nan, np.nan

            if x[-2:] in ('kg', 'ml', 'oz'):
                unit = x[:-2]
                weight = x[-2:]
            elif x[-2:] == ' .':
                unit = x[:-4]
                weight = x[-4:-2]
            elif x[-1:] == 'g':
                unit = x[:-1]
                weight = x[-1:]
            else:
                return np.nan, np.nan

            return unit, weight
        except TypeError:
            return np.nan, np.nan
    
    def _convert_product_weights(self):
        '''
        Function to take the weight column and split it then convert all values into kg then drop the added columns
        
        Parameters
        ----------
        None
        
        Returns 
        -------
        self.table
        '''
        #convert to string
        self.table['weight'] = self.table['weight'].astype('string')
        
        #drop null values
        self.remove_nulls()
        
        #create a tuple of the weights and the units
        self.table['weights_tuple'] = self.table['weight'].apply(self._split_weight_units)
        self.table['weights'] = self.table['weights_tuple'].str[0]
        self.table['unit'] = self.table['weights_tuple'].str[1]
        
        #Find weights with multiplications in and convert to kg
        self.table.loc[self.table['weights'].str.match(r'\d+ x \d+', na=False), 'weights'] = (self.table.loc[self.table['weights'].str.match(r'\d+ x \d+', na=False), 'weights'].str.extract(r'(\d+) x (\d+)').astype(int).prod(axis=1)) / 100
        
        #convert all to floats and then convert to Kg
        self.table['weights'] = self.table['weights'].astype('float')
        self.table.loc[self.table['unit'] == 'g', 'weights'] /= 1000
        self.table.loc[self.table['unit'] == 'ml', 'weights'] /= 1000
        self.table.loc[self.table['unit'] == 'oz', 'weights'] /= 35.274
        
        #replace the original weight column with the clean one and remove new columns
        self.table['weight'] = self.table['weights']
        self.table.drop(columns=['weights', 'unit', 'weights_tuple'], inplace=True)
        
        return self.table
    
    def _clean_currency(self, product_price_column:str):
        '''Replace "£", "," and "$" with nothing.
        Convert values to numeric.
        
        Parameters
        ----------
        product_price_column(str) : Column name
        
        Returns 
        -------
        self.table'''
        self.table[product_price_column] = self.table[product_price_column].str.replace('£', '').str.replace(',', '').str.replace('$', '')
        self.table[product_price_column] = pd.to_numeric(self.table[product_price_column], errors='coerce')
        return self.table
    
    def _validate_removed(self, removed_column_name: str):
        '''
        Checks the values in the removed column are either removed or still_avaliable.
        
        Parameters
        ----------
        removed_column_name(str) : column name
        
        Returns 
        -------
        self.table
        '''
        
        valid_values = ['Still_avaliable', 'Removed']
        self.table = self.table[self.table[removed_column_name].isin(valid_values)]
    
    def clean_products_data(self):
        '''Clean the product details table.
        Set correct data types for products table.
        Converts and corrects the weights column.
        Validate currency column.
        Drop nulls.
        
        Parameters
        ----------
        None
        
        Returns 
        -------
        self.table'''
        
        #drop null values
        self.remove_nulls()
        
        #correct data types
        self.table['product_name'] = self.table['product_name'].astype('string')
        self.table['category'] = self.table['category'].astype('string')
        self.table['date_added'] = pd.to_datetime(self.table['date_added'], infer_datetime_format=True, errors='coerce')
        self.table['EAN'] = self.table['EAN'].astype('string')
        self.table['uuid'] = self.table['uuid'].astype('string')
        self.table['removed'] = self.table['removed'].astype('string')
        self.table['product_code'] = self.table['product_code'].astype('string')
        
        #Validate removed column
        self._validate_removed('removed')
                
        #Convert and correct the weights column
        self._convert_product_weights()
        
        #remove currency symbols 
        self._clean_currency('product_price')
        
        
        
        return self.table

    def clean_order_data(self):
        '''Clean the orders table.
        Function to set index to index column and then strip out "first_name", "last_name", "1" and "level_0" columns.
        
        Parameters
        ----------
        None
        
        Returns 
        -------
        self.table'''
        self.table.set_index('index', inplace=True)
        self.table.drop(columns=['first_name', 'last_name', '1', 'level_0'], inplace=True)
        return self.table
        
    def clean_events_data(self):
        '''Clean the events table.
        Function to validate the time periods are "Evening", "Midday", "Morning" or "Late_Hours"
        
        Parameters
        ----------
        None
        
        Returns 
        -------
        self.table'''
        valid_time_periods = ['Evening', 'Midday', 'Morning', 'Late_Hours']
        self.table = self.table[self.table['time_period'].isin(valid_time_periods)]
        return self.table
