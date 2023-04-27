# Current problem: data table may not be efficient
# The nature of data: web API with nested structure: everytime a request returns 3 keys, all the information is contained under "entity";


library(mongolite)
library(tidyverse)

connection_string = "mongodb+srv://Anita:Y6cF7ICXqbZyFgwL@cluster0.7gfdq86.mongodb.net/test"
bus = mongo(collection="AT_Bus", 
            db="AT_Bus", url=connection_string)
count <- bus$aggregate('[{"$unwind": "$entity"}, {"$match":
{"entity.alert":{"$exists":false}}},
                       {"$group": {"_id": null, "count": {"$sum": 1}}} ]')
# total number
count2 <- bus$aggregate('[{"$unwind": "$entity"},{"$group": {"_id": null, "count": {"$sum": 1}}} ]')

#    user  system elapsed  9.17    1.96   95.50 
system.time(docs <- bus$aggregate('[{"$unwind": "$entity"},{"$match":
{"entity.alert":{"$exists":false}}},
  {"$project": {"_id": 0,"header":0}}]'))
docs<-docs$entity
names(docs)

docs<-select(docs,-"is_deleted")
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
df<-cbind(docs$id,trip_update,vehicle)
names(df)[1]<-"id"
#rm(vehicle,trip_update)
na_counts <- df %>%
  summarise_all(~ sum(is.na(.)))
#--------------
# task: 1. attach stop latitude 2. format timestamp 
#3. weather (extra: road works,duplicate,transit lane)
df$vehicle_id <- coalesce(df$t.vehicle_id,df$vehicle_id)
df$trip_id <- coalesce(df$trip_id,df$vehicle_trip_id)
df$start_time <- coalesce(df$trip_start_time,df$vehicle_start_time)
df$start_date <- coalesce(df$trip_start_date,df$vehicle_start_date)
df$schedule_relationship <- coalesce(df$trip_schedule_relationship, 
                                                df$schedule_relationship,
                                                df$vehicle_schedule_relationship)
df$route_id <- coalesce(df$trip_route_id,df$vehicle_route_id)
df$direction_id <- coalesce(df$trip_direction_id,df$vehicle_direction_id)
df$timestamp <- coalesce(df$timestamp,df$vehicle_timestamp)
df$vehicle_label <- coalesce(df$t.vehicle_label,df$vehicle_label)

df<-df[,-c(3:7,19:21,27,30:36)]
na_counts2 <- df %>%
  summarise_all(~ sum(is.na(.)))
# route id and stop id contains different format

library(data.table)

stop_data<-read.delim("E:/Bus_data/stops.txt",sep = ",")

# some stop_id are in different format, need to count them out:
count <- sum(grepl("v104.61", combined_data$stop_id))
combined_data$y <- ifelse(grepl("v104.61", combined_data$stop_id), 
                          combined_data$stop_id, NA)
combined_data$x <- ifelse(grepl("v104.61", combined_data$stop_id), 
                          NA, combined_data$stop_id)
stop_align<-matrix(data = NA,nrow=nrow(combined_data),ncol=5)
stop_align[,1]<-combined_data$x
combined_data<-combined_data%>%separate(y,c("stop_code",NA,NA),
                                        sep = "-",)
stop_align[,2]<-combined_data$stop_code

# Convert data.frames to data.tables
stop_align_dt <- data.table(stop_align)
stop_data_dt <- data.table(stop_data)
names(stop_align_dt)<-c("stop_id","stop_code","stop_lat", 
                        "stop_lon", "stop_name")

# Set the key for join operation
setkey(stop_data_dt, stop_id)

# Convert stop_align_dt to character data type
stop_align_dt[, c("stop_lat", "stop_lon","stop_name") := lapply(.SD, as.character), 
              .SDcols = c("stop_lat", "stop_lon","stop_name")]

# Perform a left join to update matching rows in stop_align_dt
stop_align_dt[stop_data_dt, on = "stop_id", `:=` (
  stop_lat = as.double(i.stop_lat),
  stop_lon = as.double(i.stop_lon),
  stop_name = as.character(i.stop_name)
), by = .EACHI]

stop_data_dt$stop_code<-as.character(stop_data_dt$stop_code)
setkey(stop_data_dt, stop_code)
stop_align_dt[stop_data_dt, on = "stop_code", `:=` (
  stop_lat = as.double(i.stop_lat),
  stop_lon = as.double(i.stop_lon),
  stop_name = as.character(i.stop_name)
), by = .EACHI]

# Convert stop_align_dt back to numeric data type
stop_align_dt[, c("stop_lat", 
                  "stop_lon") := lapply(.SD, as.numeric), 
              .SDcols = c("stop_lat", "stop_lon")]
stop_align_df<-as.data.frame(stop_align_dt)

stop<-matrix(data = NA,nrow=nrow(combined_data),ncol=2)
stop[,1]<-stop_align_dt[,2]
stop_dt <- data.table(stop)
names(stop_dt)<-c("stop_code","stop_id")
setkey(stop_data_dt, stop_code)
stop_dt[stop_data_dt, on = "stop_code", `:=` (
  stop_id = as.double(i.stop_lat)), by = .EACHI]


bus_data<-cbind(combined_data,stop_align_df[,3:5])
bus_data<-bus_data%>%mutate(stop_)
bus_data$stop_id<-stop_dt[,2]
