For each calculation, I have filtered out any report data that had a quality flag with it.

### 1) Number of stations in Texas
   
    Assumption: Only counting stations that have "TX" as their state value

    Answer: 4736

### 2) Number of reporting Texas stations in 2017

    2487

### 3) Highest tmp, location and date

    Iran, June 28, 52.9 degrees celsius. 

    That's pretty hot, but it is about 4 degrees celsius cooler than the hottest temperature ever recorded, 56.7 C. It's also the Middle East during the summer, so it seems like a plausible temperature. 


### 4) Number of unreporting stations in 2017

    66450

### 5) Max rainfall for any Texas station
  
    Station USW00012917, August 29, 66.12 centimeters

    That is a huge amount of rain, especially for Texas, but this was when Hurrican Harvey hit.

### 6) Max rainfall for any India station
  
    Station IN012201300, August 19, 37.39 centimeters

    This is kind of a lot of rain, but monsoon season runs into August, so it makes sense.

### 7) Stations associated with San Antonio
  
    Assumption: "Associated with San Antonio" means the station's name includes the string "SAN ANTONIO"

    30

### 8) San Antonio stations that reported data in 2017
  
    4

### 9) Largest daily increase for high temps in San Antonio
  
    I found the max increase for each individual station, then took the max increase of all the stations.

    12.2 degrees celsius

### 10) Correlation Coefficient for rain and high temps
  
    I calculated the coefficient for each station, then took the average of those coefficients:

    -0.143

### 11) Plot of 5 stations:

- CA005062040  53.9667  -97.8500  224.0 MB NORWAY HOUSE
- USC00081306  25.6719  -80.1567    1.8 FL CAPE FLORIDA
- USC00364778  40.1192  -76.4264  109.7 PA LANDISVILLE 2 NW
- MGM00044287  46.1330  100.6830 1859.0    BAYANHONGOR
- SPE00156540  41.6739    1.7678  349.0    SANT SALVADOR DE GUARDIOLA

![Plot](src/main/scala/sparkrdd/plot)

It bothers me that Canada has leaps of around 30 degrees celsius between days towards the beginning of the year, but I don't know enough about Canada's climate to determine if that's an anomoly. I'm guessing it could have been a snowstorm that caused a dip. All the other data is very plausible, so I know the leaps are not from incorrect plotting.  