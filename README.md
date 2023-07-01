# Bus_data
This is a project that uses AT Developer Portal's API(updated every 30 seconds) to predict the on-time behaviour of the Auckland bus. The codes and data here show the data capturing and cleaning in Python and R, the R shiny codes and the function used that create a dashboard, and the data used in the analysis and dashboard.

A **descriptive data showcase** can be found: https://anitakoh23897.shinyapps.io/bus_rt/

**The main body of analysis**: https://rpubs.com/Anita_0736

**store_python.py**: Python codes that capture the data. It was running on Google VPS previously.

**Data_Cleaning.R**: It includes 4 main parts: 
    1. Elementary data cleaning: querying, unlisting and extract the information

    2. Match the stop information for stop and weather with data.table

    3. Adding weekday with lubridate

    4. Transform spatial data with sf: within 500 meters as "in"

**Data_Cleaning.R**: Shiny dashboard codes.

**Get_data.R**: the function used to clean real-time data for the Shiny app. Similar to Data_Cleaning.R.

**des_plotting.RData**: data used in the Shiny app for historical data.

**AT data.RData**: the data from 2022.01.01 to 2023.03.23 requested from AT. Not used in the main analysis, but may of potential use.


