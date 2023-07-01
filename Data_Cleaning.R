# This is the main code used to clean the data

# First Part: data acquisition ----
library(mongolite)
library(tidyverse)

connection_string = "mongodb+srv://<username>:<passwords>@cluster0.7gfdq86.mongodb.net/test"
# for confidentiality reason, the user name and passwords are hidden

bus = mongo(collection="AT_Bus", 
            db="AT_Bus", url=connection_string)
count <- bus$aggregate('[{"$unwind": "$entity"}, {"$match":
{"entity.alert":{"$exists":false}}},
                       {"$group": {"_id": null, "count": {"$sum": 1}}} ]')
# total number
count2 <- bus$aggregate('[{"$unwind": "$entity"},{"$group": {"_id": null, "count": {"$sum": 1}}} ]')

system.time(docs <- bus$aggregate('[{"$unwind": "$entity"},{"$match":
{"entity.alert":{"$exists":false}}},
  {"$project": {"_id": 0,"header":0}}]'))
docs<-docs$entity
names(docs)

# Data wrangling
docs<-select(docs,-"is_deleted") # delete schedules that didn't exist

# Separate main categories into seperate dataframes, clean and rename it, before coalescing them to have complete information
vehicle<-docs$vehicle 
trip_update<-docs$trip_update
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
bus.df<-cbind(docs$id,trip_update,vehicle)
names(bus.df)[1]<-"id"
#rm(vehicle,trip_update)

# elementary data wrangling (coalscing) ----
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
# delete repeated columns, only keep columns that have the combined information
bus.df<-bus.df[,-c(3:7,19:21,27,30:36)]

# only obtained data that is not delayed. The main reason should refer to the NA analysis in the third section
nbus.df<-bus.df[!is.na(bus.df$delay),]
nbus.df %>%
  summarise_all(~ sum(is.na(.)))
# route id and stop id contains different format
rm(vehicle,trip_update,docs)
gc()

# stop latitude and stop longitude ----
# main idea: the stops can be matched with either stop_id and stop code(needs to extract it from a long string)
library(data.table)

# Use fread() function to read the data efficiently
stop_data <- fread("E:/Bus_data/stops.txt", sep = ",")

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
  mutate(delay_min)= delay/60)%>%
  separate(start_time,paste0("start","_",c("hour","minute",NA)),sep = ":",
           convert = T,remove = F)%>%
  select(-start_NA)%>%
  mutate(public_holiday=ifelse(nbus.df$start_date=="2023-04-25",1,0))
nbus.df$public_holiday=ifelse(nbus.df$weekday=="Sunday",1,
                              nbus.df$public_holiday)
nbus.df<-nbus.df[nbus.df$start_data!=any(c("2023-05-07","2023-05-08")),]

# weather ----
# main idea: compare if the date is the same; if so, compare hour, and paste corresponding weather conditions
weather <- read.csv("F:/Books/journals/Auckland,New Zealand 2023-04-20 to 2023-05-06.csv") 
# extract the date component
weather$date <- substr(weather$datetime, 1, 10)
# extract the time component and remove the "T" character
weather$time <- gsub("T", "", substr(weather$datetime, 12, 19))
weather<-weather[,-1]
weather<-weather%>%
  separate(time,c("hour",NA,NA),sep = ":",convert = T,remove = F)
weather.dt<-data.table(weather)
weather.dt$hour<-as.double(weather.dt$hour)
nbus.df$hour<-nbus.df$start_hour
nbus.df$date<-as.character(nbus.df$start_date)
nbus.df$hour<-ifelse(nbus.df$start_minute>30,nbus.df$hour+1,nbus.df$hour)
nbus.df$hour<-ifelse(nbus.df$hour>24, 0,nbus.df$hour)
nbus.df <- merge(nbus.df, weather.dt, 
                   by = c("date", "hour"), all.x = TRUE)
rm(weather.dt)
nbus.df<-nbus.df%>%select(-c(time,hour,date))
rm(weather)
nbus.df<-nbus.df[,-c(14:18,21,39,41)]
gc()

# spatial data ----
library(sf)
rw.sf <- st_read("F:/Books/journals/Roadworks.geojson")
ts.sf<-st_read("F:/Books/journals/Transit_Lanes.geojson")

# Convert points data frame to an sf object
bus.cods<-nbus.df[complete.cases(nbus.df$stop_lat),]
stop.sf <- st_as_sf(bus.cods, coords = c("stop_lon", "stop_lat"), 
                    crs = st_crs(ts.sf))


#Transit
# Join the points to the shape file
stops_joined_tr <- st_join(stop.sf, ts.sf)

# Find the nearest features in the shape file for each point
nearest_features_tr <- st_nearest_feature(stop.sf, ts.sf)

# Check if points are neighboring (within 500 meters)
stops_neighbors_tr <- which(nearest_features <= 500)

#Roadworks
# Join the points to the shape file
stops_joined_rw <- st_join(stop.sf, rw.sf)

# Find the nearest features in the shape file for each point
nearest_features_rw <- st_nearest_feature(stop.sf, rw.sf)

# Check if points are neighboring (within 500 meters)
stops_neighbors_rw <- which(nearest_features <= 500)

bus.cods$order<-c(1:nrow(bus.cods))
bus.cods$roadwork<-ifelse(bus.cods$order%in%stops_neighbors_rw,1,0)
bus.cods$is_transit<-ifelse(bus.cods$order%in%stops_neighbors_tr,1,0)


# final modelling data ----
df<-select(bus.cods,-c("trip_id","timestamp","arrival_delay","arrival_time","arrival_uncertainty","depature_delay","depature_time","depature_uncertainty","stop_id","vehicle_id","vehicle_label","start_time","start_date","route_id","stop_name","delay_min","feelslike"))
df$start_hour<-ifelse(df$start_hour>23,df$start_hour-24,df$start_hour)
df$start_time<-df$start_hour+df$start_minute/60
df<-select(df,-c(start_hour,start_minute))
df<-df[!is.na(df$temp),]
