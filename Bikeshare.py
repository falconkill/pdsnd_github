"""
Programmer: Charles Brian MacConney
Date: 04/24/2022
Course: Python Programming for Data Science
Institution: Udacity
Updated lines: Suspect birth year - Broke up lines 284-285, 382-383 and 388-389
"""

import time
import pandas as pd
import os


# Global Constants
cwd = os.getcwd()
CITY_DATA = {'chicago': 'chicago.csv',
             'new york city': 'new_york_city.csv',
             'washington': 'washington.csv'}

LST_VALID_CITIES = ['CHICAGO', 'NEW YORK CITY', 'WASHINGTON']

LST_VALID_MONTHS = ['ALL', 'JAN', 'FEB', 'MAR', 'APR',
                    'MAY', 'JUN']

LST_VALID_DAYS = ['ALL', 'M', 'T', 'W', 'TH', 'F', 'SA', 'SU']


# Functions
def check_for_supporting_files(CITY_DATA, cwd):
    """
    Ensures that csv files exist in the common working directory for Chicago,
    New York City and Washington. Returns a list of missing files to main if files
    are not found.

    Args:
        (dict) CITY_DATA - A dictionary of city to source file (*.csv)
        (str) cwd - Current Working Directory (provided by os package).

    Returns:
        (str) str_msg - Customer file not found message (appends for each missing file).
        (bln) missing file flag - Sets flag to exit the program on return to main.
    """


    # Intialize function variables
    str_msg = ''
    bln_missing_file_flg = False

    # Check the common working directory to ensure supporting files exist.
    print('Hello! Let\'s explore some US bikeshare data!')
    print('Before starting, we need to make sure that the supoprting files exist.')
    print('The Python script and its supporting files must be in the same directory. \n')
    print(f'Checking {cwd} for supporting data (*.csv) files...\n')
    print('-'*40)
    for key, value in CITY_DATA.items():
        file_name = cwd + '\\' + value
        key = key.title()
        print(f'Checking {key}')
        try:
            int_file_size_byte = os.stat(file_name).st_size
            print(f'{key} located in {file_name}')
            mb_file_size = int(int_file_size_byte/1024/1024)
            print(f'File size = {mb_file_size} mb.\n')
        except:
            str_msg += f'{file_name}\n'
            bln_missing_file_flg = True
            continue

    if bln_missing_file_flg is False:
        print(f'Success! All files found in {cwd}. Let\'s get started.')

    print('-'*40)

    return str_msg, bln_missing_file_flg


def get_user_input(str_filter_group):
    """
    Validates (str) user input based on selection (e.g. city, month or day).

    Args:
        (str) str_filter_group - Group to be filetered (e.g. city, month or day).
        as string.

    Returns:
        (str) user_input - Validated user response.
    """
    # Initialize test list.
    lst_test = []

    # Assign correct validaton to test_list based on str_filter group.
    if str_filter_group == 'city':
        lst_test = LST_VALID_CITIES
    elif str_filter_group == 'month':
        lst_test = LST_VALID_MONTHS
    elif str_filter_group == 'day':
        lst_test = LST_VALID_DAYS

    # Get user value and validate against cities, months, and days lists.
    while True:
        user_input = input(f'Please select a {str_filter_group} from the following list {lst_test}. ')
        if user_input.upper().strip() not in lst_test:
            print('Incorrect entry. Please try again.')
            continue
        break
    user_input = user_input.upper().strip()
    print(f'{user_input} is a valid {str_filter_group}')
    return user_input


def get_filters():
    """
        Passes values to get_user_input for validation.

    Args: None

    Returns:
        (str) city - Valid name of the city to analyze.
        (str) month - Valid name of the month to filter by, or "all" to apply no month filter.
        (str) day - Valid name of the day of week to filter by, or "all" to apply no day filter.
    """
    # get user input for city (chicago, new york city, washington).
    city = get_user_input('city')

    # get user input for month (all, january, february, ... , june)
    month = get_user_input('month')

    # get user input for day of week (all, monday, tuesday, ... sunday)
    day = get_user_input('day')

    # Summarize fileters
    print()
    print('-'*40)
    print('You selected the following filters: ')
    print(f'\tCity --> {city}')
    print(f'\tMonth --> {month}')
    print(f'\tDay --> {day}')
    print('-'*40)

    # Pack and return values for city, month and day to calling method
    return city, month, day


def load_data(city):
    """
    Loads data for the city selected by the user.

    Args:
        (str) city - Name of the city selected by user.

    Returns:
        df_city - Pandas DataFrame containing raw city data.
    """
    # Import pandas dataframe.
    file_name = CITY_DATA[city.lower()]
    file_path = cwd + '\\' + file_name
    print(f'Importing data from {file_path}...')
    df_city_raw = pd.read_csv(file_path)

    return df_city_raw


def add_calc_columns(city, df_city_raw):
    """
    Formats Start Time and End Time as datetime data types and adds Start Month
    Start Hour, Start Day Name, Start Day Number, End Hour and End Day to df_city_raw.

    Input:
        (str) city - City selected by user.
        as string.

    Returns:
        (df) df_city_raw - Pandas DataFrame with additional (formatted) columns.
    """

    # Parse Start and Stop times.
    print('Parsing start and stop times...')
    df_city_raw['Start Time'] = pd.to_datetime(df_city_raw['Start Time'])
    df_city_raw['Start Month'] = df_city_raw['Start Time'].dt.month
    df_city_raw['Start Hour'] = df_city_raw['Start Time'].dt.hour
    df_city_raw['Start Day Name'] = df_city_raw['Start Time'].dt.day_name()
    df_city_raw['Start Day Number'] = df_city_raw['Start Time'].dt.day
    df_city_raw['End Time'] = pd.to_datetime(df_city_raw['End Time'])
    df_city_raw['End Hour'] = df_city_raw['End Time'].dt.hour
    df_city_raw['End Day'] = df_city_raw['End Time'].dt.day_name()

    total_imported_records = len(df_city_raw)
    city = city.title()
    print(f'Total records retrieved for {city} --> {total_imported_records}')

    print('Import complete.\n')

    return df_city_raw


def filter_df_city(df_city_load, month, day):
    """
    Applies filters to df_city Pandas DataFrame based on user inputs.

    Args:
        (df) df_city_loaded - Pandas DataFrame containing city data.
        (str) month - name of the month to filter by, or "all" to apply no month filter.
        (str) day - name of the day of week to filter by, or "all" to apply no day filter.

    Returns:
        (df) df_city-load - Pandas DataFrame containing city data filtered by month and day.
        (str) filter_msg - User message that communcates an error with filtered data.
    """

    filter_msg = ''
    print('Filtering data...')
    # filter by month if applicable
    if month != 'ALL':
        # use the index of the months list to get the corresponding int
        lst_months = LST_VALID_MONTHS[1:13]
        int_month = lst_months.index(month) + 1
        # filter by month to create the new dataframe
        df_city_load = df_city_load[df_city_load['Start Month'] == int_month]

    # filter by day of week if applicable
    if day != 'ALL':
        # use the index of the days list to get the corresponding int
        lst_days = LST_VALID_DAYS[1:8]
        int_day = lst_days.index(day)-1
        df_city_load = df_city_load[df_city_load['Start Day Number'] == int_day]

    # Handle case where user enters ALL for both filters
    if month == 'ALL' and day == 'ALL':
        print('ALL months and ALL Days are in the data - No filters applied.')
        return df_city_load, filter_msg

    # Handle case where arell records are filtered out of Pandas DataFrame
    if len(df_city_load) == 0:
        filter_msg = 'All values filtered out... No data available. Please try again.'
        print(filter_msg)
        return df_city_load, filter_msg

    print('Filter complete.\n')

    return df_city_load, filter_msg


def get_most_frequent(str_city, df_city_filtered, str_txt, df_column):
    """
    Common function used to calculate metrics based on statistical mode.

    Args:
        (str) str_city - String value of city selected by user.
        (df) df_city_filtered - Pandas DataFrame filtered to month and day as
             selected by user.
        (str) str_text - Identifier of type of data day, hour or month.
        (str)df_column - Specific column in df_city_filtered for calculation.

    Returns:
        None - Print function only
    """
    title_str_txt = str_txt.title()
    try:
        most_frequent = df_city_filtered[df_column].mode()[0]

    except Exception:
        most_frequent = 'No valid data for this filter'

    # Report and format most common month
    if str_txt == 'month':
        str_most_frequent = LST_VALID_MONTHS[most_frequent].title()
        print(f'{title_str_txt} --> {str_most_frequent}')

    # Report and format most common hour
    elif str_txt == 'hour':
        if most_frequent > 12:
            str_hour = str(most_frequent - 12)
            str_most_frequent = str_hour + ':00 pm'
            print(f'{title_str_txt} --> {str_most_frequent}')
        else:
            str_hour = str(most_frequent)
            str_most_frequent = str_hour + ':00 am'
            print(f'{title_str_txt} --> {str_most_frequent}')

    # Format Birth Year
    elif str_txt == "birth year":
        int_most_frequent = int(most_frequent)
        str_most_frequent = str(int_most_frequent)
        print(f'Most common {title_str_txt} --> {str_most_frequent}')

    # Default - Report other (including Day)
    else:
        print(f'{title_str_txt} --> {most_frequent}')
    return


def time_stats(city, df_city_filtered):
    """Displays statistics on the most frequent times of travel."""
    print(f'Calculating the most popular travel times for {city}...')
    start_time = time.time()

    # Display the most common month
    get_most_frequent(city, df_city_filtered, 'month', 'Start Month')

    # Display the most common day of week
    get_most_frequent(city, df_city_filtered, 'day', 'Start Day Name')

    # Display the most common start hour
    get_most_frequent(city, df_city_filtered, 'hour', 'Start Hour')

    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*40)


def station_stats(city, df_city_filtered):
    """Displays statistics on the most popular stations and trip."""
    print('\nCalculating The Most Popular Stations and Trips...\n')
    start_time = time.time()

    # display most commonly used start station
    get_most_frequent(city, df_city_filtered, 'start station', 'Start Station')

    # display most commonly used end station
    get_most_frequent(city, df_city_filtered, 'end station', 'End Station')

    # display most frequent combination of start station and end station trip.
    df_city_filtered['Trip'] = df_city_filtered[['Start Station',
                                                 'End Station']].agg(' to '.join, axis=1)
    get_most_frequent(city, df_city_filtered, 'trip', 'Trip')

    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*40)


def trip_duration_stats(city, df_city_filtered):
    """Displays statistics on the total and average trip duration."""

    print('\nCalculating Trip Duration Statistics...\n')
    start_time = time.time()

    # display total travel time
    total_travel_time = df_city_filtered['Trip Duration'].sum()
    total_travel_time_hours = total_travel_time / 60 / 60
    print('Total Travel Time --> {:.2f} hours'.format(total_travel_time_hours))

    # display mean travel time
    mean_travel_time = df_city_filtered['Trip Duration'].mean()
    mean_travel_time_mins = mean_travel_time / 60
    print('Mean Travel Time --> {:.2f} minutes'.format(mean_travel_time_mins))

    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*40)


def user_stats(city, df_city_filtered):
    """Displays statistics on bikeshare users."""

    print('\nCalculating User Statistics...\n')
    start_time = time.time()

    # Display counts of user types
    print('Summary by User Type:')
    user_types = df_city_filtered['User Type'].value_counts()
    print(user_types)
    print()

    # Display counts of gender

    print('Summary by Gender:')
    if city.upper() == 'WASHINGTON':
        gender_types = 'Gender Data Not Available'
    else:
        gender_types = df_city_filtered['Gender'].value_counts()
    print(gender_types)
    print()

    # Display earliest, most recent, and most common year of birth

    print('Summary by Birth Date:')
    if city.upper() == 'WASHINGTON':
        print('Birth Date Data Not Available')
        return

    # Earliest Year
    min_birth_year = (df_city_filtered['Birth Year'].min())
    earliest_birth_year = int(min_birth_year)
    print(f'Earliest Birth Year --> {earliest_birth_year}')

    # Most Recent Year
    max_birth_year = (df_city_filtered['Birth Year'].max())
    int_latest_birth_year = int(max_birth_year)
    print(f'Most Recent Birth Year --> {int_latest_birth_year}')

    # Most Common Year
    get_most_frequent(city, df_city_filtered,
                      'birth year',
                      'Birth Year')

    print("\nThis took %s seconds." % (time.time() - start_time))
    print('-'*40)


def view_data_records(city, month, day, df_city_raw, df_city_filtered):
    """
    Provides the user with the ability to view raw data 5 records at a time.

    Input:
        (str) city - City selected by user.
        (str) month  - Selected by user.
        (str) day  - Day selected by user.
        (df) df_city_raw - All raw data selected by user.
        (df) df_city_filtered - Filtered raw data selected by user.

    Returns:
        (str) user_input - Validated user response.
    """
    user_view = input('Do you want to view raw data records (Press any key for YES or "N" for NO)? ')
    if user_view.upper() == 'N':
        return

    # Allow user to select All Data or Filtered Data for Display
    while True:
        print('Would you like to see all raw data or filtered raw data? ')
        print('Enter "1" for all raw data or ')
        print('Enter "2" for filtered data or ')
        user_sel = input('Please enter "1" or "2". ')
        if user_sel == '1':
            sel_df = df_city_raw
            break
        elif user_sel == '2':
            sel_df = df_city_filtered
            print('You selected the filtered view. Here are your filters: ')
            print(f'City --> {city}')
            print(f'Month --> {month}')
            print(f'Day --> {day}')
            sel_df = df_city_filtered
            break
        else: continue

    print('\nTotal records in selection --> {}.\n'.format(len(sel_df)))

    print(f'Here is the raw data for {city} printed 5 records at a time.')

    # loop through dataframe 5 records per iteration and display to user
    print('Fetching raw data...')
    user_inp = ''
    start = 0
    stop = 5
    while start <= len(sel_df):
        print(f'Printing records {start} to {stop}')
        print(sel_df.iloc[start:stop])
        if stop > len(sel_df):
            print('End of file.')
            break
        user_inp = input('Print the next 5 records (Press any key to continue or "N" to quit)? ')
        if user_inp.upper() == 'N':
            break
        start = stop
        stop = start + 5

    return


# Main Method
def main():
    str_msg, bln_missing_file= check_for_supporting_files(CITY_DATA, cwd)
    if bln_missing_file is True:
          msg = f'The following files are missing from the common working directory ({cwd}):\n\n' + str_msg
          msg += f'\nPlease add these files to {cwd} and try again.'
          raise Exception(msg)

    while True:
        city, month, day=get_filters()
        df_city_raw = load_data(city)
        df_format_city = add_calc_columns(city, df_city_raw)
        df_city_filtered, filter_msg = filter_df_city(df_format_city, month, day)

        if filter_msg == '':
            time_stats(city, df_city_filtered)
            station_stats(city, df_city_filtered)
            trip_duration_stats(city, df_city_filtered)
            user_stats(city, df_city_filtered)
            view_data_records(city, month, day, df_city_raw, df_city_filtered)
        else:
            break

        print('\nWould you like to restart? ')
        restart = input('Enter "Y" = YES or "N" = NO. ')
        if restart.upper() == 'N':
            break
    print('Exiting program.')


if __name__ == "__main__":
    main()
