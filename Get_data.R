# The functino used for plotting real-time data in shiny dashboard
# Mostly similar to the data cleaning file

GET_data<-function(){
  # GET data ----
  library(httr)
  library(jsonlite)
  library(tidyverse)
  
  # API URL and headers
  url <- "https://api.at.govt.nz/realtime/legacy"
  key <- "6eed86a9a0984545924b74935e335283"
  headers <- c("Ocp-Apim-Subscription-Key" = key)
  
  # Send the GET request
  response <- GET(url, add_headers(headers))
  
  # Check the response status code
  status_code <- status_code(response)
  if (status_code == 200) {
    # Request was successful
    json_data <- content(response, "text") %>%
      fromJSON(simplifyVector = TRUE) %>%
      `[[`("response")
  } else {
    # Request encountered an error
    message(paste("API request failed with status code", status_code))
  }
  
  # Data Cleaning ----
  data<-json_data$entity[is.na(json_data$entity$alert$effect),]
  data<-select(data,-alert)
  
  trip_update<-data$trip_update
  vehicle<-data$vehicle
  vehicle<-vehicle%>%unnest(everything())
  names(vehicle)<-paste0("vehicle","_",names(vehicle))
  
  names(trip_update$trip)[2:6]<-paste0("trip","_",
                                       names(trip_update$trip)[2:6])
  names(trip_update$vehicle)<-paste0("t",".","vehicle","_",
                                     names(trip_update$vehicle))
  names(trip_update$stop_time_update$arrival)<-paste0(
    "arrival","_",names(trip_update$stop_time_update$arrival))
  names(trip_update$stop_time_update$departure)<-paste0(
    "depature","_",names(trip_update$stop_time_update$departure))
  trip_update$stop_time_update<-trip_update$stop_time_update%>%unnest(everything())
  trip_update<-trip_update%>%unnest(everything())
  bus.df<-cbind(data$id,trip_update,vehicle)
  names(bus.df)[1]<-"id"
  
  bus.df$vehicle_id <- coalesce(bus.df$t.vehicle_id,bus.df$vehicle_id)
  bus.df$trip_id <- coalesce(bus.df$trip_id,bus.df$vehicle_trip_id)
  bus.df$start_time <- coalesce(bus.df$trip_start_time,bus.df$vehicle_start_time)
  bus.df$start_date <- coalesce(bus.df$trip_start_date,bus.df$vehicle_start_date)
  bus.df$schedule_relationship <- coalesce(bus.df$trip_schedule_relationship, 
                                           bus.df$schedule_relationship,
                                           bus.df$vehicle_schedule_relationship)
  bus.df$route_id <- coalesce(bus.df$trip_route_id,bus.df$vehicle_route_id)
  bus.df$direction_id <- coalesce(bus.df$trip_direction_id,
                                  bus.df$vehicle_direction_id)
  bus.df$timestamp <- coalesce(bus.df$timestamp,bus.df$vehicle_timestamp)
  bus.df$vehicle_label <- coalesce(bus.df$t.vehicle_label,
                                   bus.df$vehicle_label)
  
  bus.df<-bus.df[,-c(3:7,19:21,27,30:36)]
  nbus.df<-bus.df[!is.na(bus.df$delay),]
  rm(vehicle,trip_update,data)
  gc()
  
  # Stop data-----------
  library(data.table)
  
  # Use fread() function to read the data efficiently
  #stop_data <- fread("E:/Bus_data/stops.txt", sep = ",")
  stop_data <- fread("C:/Users/awu792/Downloads/gtfs/stops.txt", sep = ",")
  
  # some stop_id are in different format, need to count them out:
  nbus.df$y <- ifelse(grepl("v104.61", nbus.df$stop_id),nbus.df$stop_id, NA)
  nbus.df$x <- ifelse(grepl("v104.61", nbus.df$stop_id), NA,nbus.df$stop_id)
  
  nbus.df<-nbus.df%>%separate(y,c("stop_code",NA,NA),
                              sep = "-",)
  stop_align <- data.table(
    stop_id = nbus.df$x,
    stop_code = as.numeric(nbus.df$stop_code),
    stop_lat = numeric(nrow(nbus.df)),
    stop_lon = numeric(nrow(nbus.df)),
    stop_name = character(nrow(nbus.df))
  )
  
  # Set the key for join operation
  setkey(stop_data, stop_id)
  setkey(stop_data, stop_code)
  
  # Perform a left join to update matching rows in stop_align
  stop_align[stop_data, on = "stop_id",`:=` (
    stop_lat = as.double(i.stop_lat),
    stop_lon = as.double(i.stop_lon),
    stop_name = as.character(i.stop_name)
  ), by = .EACHI]
  
  
  # Perform a left join to update matching rows in stop_align
  stop_align[stop_data, on = "stop_code",`:=` (
    stop_lat = as.double(i.stop_lat),
    stop_lon = as.double(i.stop_lon),
    stop_name = as.character(i.stop_name),
    stop_id = as.character(i.stop_id)
  ), by = .EACHI]
  
  nbus.dfstop_id<-stop_align$stop_id
  nbus.df<-nbus.df%>%select(-c("x","stop_code"))
  nbus.df<-cbind(nbus.df,stop_align[,3:5])
  nbus.df[,26:27][nbus.df[,26:27] == 0]<-NA
  
  rm(stop_data, stop_align)
  gc()
  
  # Time and date ----
  library(lubridate)
  nbus.df$x <- ifelse(grepl("v104.61", nbus.df$route_id),
                      nbus.df$route_id,NA)
  nbus.df<-nbus.df%>%separate(x,c("x",NA,NA),
                              sep = "-",)
  nbus.df$x <- paste0(substring(nbus.df$x, 1, 2), "-", 
                      substring(nbus.df$x, 3))
  nbus.df$route_id<-ifelse(grepl("v104.61", nbus.df$route_id),nbus.df$x,
                           nbus.df$route_id)
  nbus.df<-nbus.df%>%select(-x)
  nbus.df<-nbus.df%>%
    mutate(start_date = as.Date(as.character(start_date), 
                                format = "%Y%m%d"))%>%
    mutate(weekday=weekdays(start_date))%>%
    mutate(delay_min = delay/60)%>%
    separate(start_time,paste0("start","_",c("hour","minute",NA)),sep = ":",
             convert = T,remove = F)%>%
    mutate(s.hour_min=start_hour+start_minute/60)%>%
    select(-start_NA)
  rm(bus.df,json_data,response,nbus.dfstop_id)
  gc()
  return(nbus.df)
}
