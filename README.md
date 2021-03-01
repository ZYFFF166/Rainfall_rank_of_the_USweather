Author: Yifan Zhao 

### HOW TO RUN
A script under the file /project. To run the script, use the command “./rainfall_rank.sh [path of the Location file] [path of the Recordings files] [path of the output folder]”
For example, run in the terminal:
$ ./rainfall_rank.sh /Users/zhaoyifan/Desktop/project/Dataset/Locations /Users/zhaoyifan/Desktop/project/Dataset/Recordings /Users/zhaoyifan/Desktop/project/Dataset/output 

Find the result txt file under the path: /project/Dataset/output

### OVERALL DESCRIPTION
The goal of the project is to find out which states in the US have the most stable rainfall. The result provides the US states ordered (in ascending order) the difference between the two months with the highest and lowest rainfall. 
I divided this project into four steps. The first step is to find the locations in US, and group stations by states. The second step is to union all the recordings together. The third step is to get the month of each state with highest and lowest average precipitation. We also need to do the calculation of precipitation. Finally, we need to calculate the difference of precipitation between these two months of each state and order the states in ascending order.

### DESCRIPTION OF EACH STEP
1. The get_in_usa function is to find the stations in US. Use map function with replace function and split function to use the list to represent each station. Then, use filter function to find the station with the "CTRY" field which is the third field of the list is "US" Only keep the USAF(station ID) and the state with the map function.
2. In the get_recording function, union all the recordings. Read the four recording files from 2006 to 2009 under the recording dataset and use the union function to union all the RDD in list so that we can get the recordings from 2006 to 2009.
3. In the get_avg_prcp_min and get_avg_prcp_max function, find the lowest and highest average precipitation for each stage. First, use the join function to find the recordings of the stations in US. Then, split the column PRCP into two parts, the number part and the letter part. Use if function to check the letter according to the table and calculate the precipitation. For example, 0.26G, the calculation for the precipitation is: 0.26/24*24.Next, apply reduceByKey function to find the minimum(maximum) precipitation for each month of each stage. Last, use groupByKey function to find the min and max precipitation among the whole year of each stage.
4. The get_diff function is to calculate the difference between the highest and the lowest rainfall. It calls the calendar library to transform the month field. With sortBy function, the result can be ordered by the difference between the highest and the lowest month, in ascending order. The get_result function write the result in the output file.

### TOTAL RUNTIME
Calling the time function to calculate the runtime. The run time is 118.3s.

### EXTRA CREDIT
1. Using the join function in spark to get a quick combination of the recording file and the location file. By using the join function, we can speed up the execution.
2. Remove noise data of 99.99 in column PRCP using filter

