### Shiny App

# stage 1: using route id to plot delay time (ideally compare and contrast 
# many different route)

# stage 2: leaflet map showing the ontime behaviour of the moment
# if for the worst part of the day, have to show 1. historical data () 
# 2. use a database to update it 

library(shiny)
library(ggplot2)
library(leaflet)

#setwd("E:/Rwork/STATS765/Bus_Shiny/Bus_RT")
setwd("D:/schools/STATS765/Project/Bus_Shiny/Bus_RT")
source("GET_data.R")

# Define UI for application that draws a histogram
ui <- fluidPage(
  titlePanel("AT Real Time Bus Delay"),
  
  # Tabbed layout
  tabsetPanel(
    # Tab 1: Delay Plot----
    tabPanel(
      "Delay Plot",
      actionButton("get_data_button", "Get Data"),
      uiOutput("bus_select"),
      actionButton("add_one_button", "Add One"),
      uiOutput("additional_select"),
      plotOutput("delay_plot"),
    ),
    
    # Tab 2: Leaflet Map----
    tabPanel(
      "Leaflet Map",
      leafletOutput("map")
    )
  )
)


# Define server logic required to draw a histogram
server <- function(input, output, session) {
  bus_data <- reactiveVal(NULL)
  select_inputs <- reactiveVal(list())
  
  # tab1 ----
  observeEvent(input$get_data_button, {
    bus_data(GET_data())  # Call the function without the .R extension
  })
  
  observeEvent(input$add_one_button, {
    current_selects <- select_inputs()
    current_selects[[length(current_selects) + 1]] <- paste0("var_", length(current_selects) + 1)
    select_inputs(current_selects)
  })
  
  output$bus_select <- renderUI({
    if (is.null(bus_data())) return()
    
    selectInput(
      inputId = "bus_id",
      label = "Route:",
      choices = bus_data()$route_id,
      selected = bus_data()$route_id[1]
    )
  })
  
  observeEvent(select_inputs(), {
    new_selects <- select_inputs()
    
    lapply(seq_along(new_selects), function(i) {
      output[[new_selects[i]]] <- renderUI({
        if (is.null(bus_data())) return()
        
        selectInput(
          inputId = new_selects[i],
          label = "Additional Variable:",
          choices = setdiff(names(bus_data()$route_id), unlist(new_selects[1:i])),
          selected = NULL
        )
      })
    })
  })
  
  output$delay_plot <- renderPlot({
    if (is.null(bus_data())) return()
    
    filtered_data <- subset(bus_data(), route_id == input$bus_id)
    
    gg <- ggplot(filtered_data, aes(x = s.hour_min, y = delay_min))
    gg <- gg + geom_point(size = 3)
    gg <- gg + geom_text(aes(label = stop_name), vjust = 0, nudge_y = 0.5)
    gg <- gg + labs(title = "Delay by Start Time",
                    x = "Start time",
                    y = "Delay (minutes)")
    
    # Iterate over additional variables and map aesthetics dynamically
    for (var_input in unlist(select_inputs())) {
      if (!is.null(input[[var_input]])) {
        gg <- gg + geom_point(aes_string(color = var_input), size = 3)
        gg <- gg + labs(color = var_input)
        gg <- gg + scale_color_manual(values = rainbow(length(unique(filtered_data[[var_input]]))))
      }
    }
    
    gg + theme(legend.position = "right")
  })
  
  
  # tab2----
  output$map <- renderLeaflet({
    if (is.null(bus_data())) return()
    
    map <- leaflet() %>%
      addTiles() %>%
      setView(lng = 174.7633, lat = -36.8485, zoom = 11)
    
    palette <- colorNumeric(
      palette = c("orange", "brown1"),
      domain = c(0, max(abs(bus_data()$delay_min)))
    )
    map <- map %>%
      addCircleMarkers(data = bus_data(), 
                       lng = ~stop_lon, lat = ~stop_lat,
                       color = ~palette(abs(delay_min)),
                       radius = 5) %>%
      addLabelOnlyMarkers(data = bus_data(),
                          lng = ~stop_lon, lat = ~stop_lat,
                          label = ~route_id)
  })
}

# Run the application 
shinyApp(ui = ui, server)
         
