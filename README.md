# Knowledge-base-for-Word-Prediction

# Introduction
The main goal of this application is to generate a knowledge-base for the 
English word-prediction system, based on a Google 3-Gram English dataset, using 
Amazon Elastic Map-Reduce (EMR). The produced knowledge-base indicates the 
probability of each word trigram found in the corpus.
The corpus we used is Google Books 3-grams English data which his size is 
218.1 GB, you can find more information [here](https://aws.amazon.com/datasets/google-books-ngrams/). 
We will implement a held-out method, named deleted estimation. Held-out 
estimators divide the training data (the corpus) into two parts, build initial 
estimates by doing counts on one part, and then use the other pool of held-out 
data to refine those estimates. The deleted estimation method, for instance, 
uses a form of two-way cross-validation, as follows:
![image](https://github.com/IdanArbiv/Knowledge-base-for-Word-Prediction/assets/101040591/a4e2f09c-d603-42af-bbcf-bcdc9199384a)

- N is the number of n-gram instances in the whole corpus.
- 𝑁𝑟
0 is the number of n-gram types occurring r times in the first part of the 
corpus. 
- 𝑇𝑟
01 01 is the total number the n-grams of the first part (of Nr 0 ) appear the 
second part of the corpus (instances). 
- 𝑁𝑟
1 1 is the number of n-gram types occurring r times in the second part of 
the corpus.
- 𝑇𝑟
10 10 is the total number the n-grams of the second part (of Nr 1 ) appear in 
the first part of the corpus (instance).

You can find some more explanation on the deleted-estimation formula in sections 
6.2.3 and 6.2.4 [here](https://wiki.eecs.yorku.ca/course_archive/2014-15/W/6339/_media/chap6.pdf).

# Dependencies
Type: Large
Load POM.xml

# Short usage tutorial
1. Extract the project files into a project.
2. Load the dependency by loading the Pom.xml of each project.
3. Connect to the AWS account and copy the credentials
4. Set credentials in the AWS credentials profile file on your local system, 
located at:
~/.aws/credentials on Linux, macOS, or Unix
C:\Users\USERNAME\.aws\credentials on Windows
5. Under the AWS classes change the constants concerning REGIONS according 
to the REGIONS given to you in AWS.
The region we used:
US_EAST_1
6. Change the 'Main-Class' property under the file MANIFEST for each step.MF 
to the current step, and create a jar file.
7. Upload all the jars file to your Bucket in AWS and change the jar paths 
in the main class.
8. Create a folder called "Logs" in your bucket and change the log path in 
the main class.

# System Architecture
The system is composed of 5 elements:
o Main class
o 4 Steps
Each step is scheduled using Amazon Elastic Map-Reduce (EMR).

# Main
The purpose of the main is to initialize the EMR program, and in particular to 
define all the configurations required to run a job that includes several steps. 
Among other things, define the jar paths from which they will be run, define how 
many instances will run the program, define the log writing path, etc.
![image](https://github.com/IdanArbiv/Knowledge-base-for-Word-Prediction/assets/101040591/0b42aecc-286e-4e1f-9a85-694d9110d831)

# Step 1 –
The purpose of the first step is to calculate for each 3-grams the number of its 
appearances in the entire corpus and also in the first and second half of the 
corpus. Furthermore, we count the total number of 3-grams N.

- 'Mapper' –
Input:
Key = lineId (LongWritable)
Value = n-gram \t year \t occurrences \t pages \t books (Text)
Output:
1) Number of occurrences in each part of the corpus - Key = <w1, w2, w3> Value 
= occurrences \t corpusPart(0/1)
2) All three grams occurrences in the corpus (N) - Key = ** Value = occurrences
- 'Reducer' –
Input:
1) Number of occurrences in each part of the corpus - Key = <w1, w2, w3> Value = 
occurrences \t corpusPart(0 OR 1)
2) All three grams occurrences in the corpus (N) - Key = ** Value = occurrences
Output:
1) Key = <w1, w2, w3> Value = r \t R0 \t R1
r = Number of occurrences in all the corpus
R0 = Number Of occurrences in the first half
R1 = Number Of occurrences in the second half
2) Number of 3-grams in all the corpus - Key = **, Value = totalNumberOf3-grams
- 'Combiner' (optional) –
Input:
1) Number of occurrences in each part of the corpus - Key = <w1, w2, w3> Value = 
occurrences \t corpusPart(0 OR 1)
2) All three grams occurrences in the corpus (N) - Key = ** Value = occurrences
Output:
1) Key = <w1, w2, w3> Value = R0 \t 0
2) Key = <w1, w2, w3> Value = R1 \t 1
R0 = Number Of occurrences in the first half
R1 = Number Of occurrences in the second half
3) Number of 3-grams in all the corpus - Key = **, Value = totalNumberOf3-grams

Output screenshot -
![image](https://github.com/IdanArbiv/Knowledge-base-for-Word-Prediction/assets/101040591/b2c47ad4-83da-43c5-a580-a19527e60809)

# Step 2 –
The purpose of step 2 is to produce all the parameters required for calculating the 
probability of a given 3-grams. Also, "match" the total number of 3-grams in the 
corpus - N for each r received.
- 'Mapper' –
Input:
1) Key = lineId Value = <w1 w2 w3 \t r \t R1 \t R2>
2) Key - lineId Value = <** \t occurrences>
Output:
1) Key = <** **> Value = occurrences (Number of total 3-grams in all the corpus)
2) Key = <w1 w2 w3> Value = r
3) Key = <N 0 R0> Value = 1 (types)
4) Key = <T 1 R0> Value = R1 (instances)
5) Key = <N 1 R1> Value = 1 (types)
6) Key = <T 0 R1> Value = R0 (instances)7) Key = <**, r> Value = r (For each 
<w1,w2,w3> that in the input)
- 'Reducer' –
Input:
1) Key = <** **> Value = occurrences (Number of total 3-grams in all the corpus)
2) Key = <w1 w2 w3> Value = r
3) Key = <N 0 R0> Value = 1 (types)
4) Key = <T 1 R0> Value = R1 (instances)
5) Key = <N 1 R1> Value = 1 (types)
6) Key = <T 0 R1> Value = R0 (instances)
7) Key = <**, r> Value = r (For each <w1,w2,w3> that in the input)
Output:
1) Key = <** r> Value = occurrences (Number of total 3-grams in all the corpus)
2) Key = <w1 w2 w3> Value = r
3) Key = <N 0 r> Value = occurrences (Sum of types) - N_r_0
4) Key = <N 1 r> Value = occurrences (Sum of types) - N_r_1
5) Key = <T 0 r> Value = occurrences (Sum of instances) - T_r_01
6) Key = <T 1 r> Value = occurrences (Sum of instances) - T_r_10
- 'Combiner' (optional) –
Input:
1) Key = <** **> Value = occurrences (Number of total 3-grams in all the corpus)
2) Key = <w1 w2 w3> Value = r
3) Key = <N 0 R0> Value = 1 (types)
4) Key = <T 1 R0> Value = R1 (instances)
5) Key = <N 1 R1> Value = 1 (types)
6) Key = <T 0 R1> Value = R0 (instances)
7) Key = <**, r> Value = r (For each <w1,w2,w3> that in the input)
Output:
1) Key = <** **> Value = occurrences (Number of total 3-grams in all the corpus)
2) Key = <w1 w2 w3> Value = r
3) Key = <N 0 r> Value = occurrences (Sum of types) - N_r_0
4) Key = <N 1 r> Value = occurrences (Sum of types) - N_r_1
5) Key = <T 0 r> Value = occurrences (Sum of instances) - T_r_01
6) Key = <T 1 r> Value = occurrences (Sum of instances) - T_r_10
7) Key = <**, r> Value = r (For each <w1,w2,w3> that in the input)

Output screenshot -
![image](https://github.com/IdanArbiv/Knowledge-base-for-Word-Prediction/assets/101040591/08a19a67-d027-44dd-a0d7-43a7a4364649)


# Step 3 –
The purpose of step 3 is to unite for a specific r all the parameters required 
to calculate a probability, then send all the relevant 3-grams to that r and 
generate the appropriate probability
- 'Mapper' –
Input:
1) Key = <** r> Value = occurrences (Number of total 3-grams in all the corpus)
2) Key = <w1 w2 w3> Value = r
3) Key = <N 0 r> Value = occurrences (Sum of types) - N_r_0
4) Key = <N 1 r> Value = occurrences (Sum of types) - N_r_1
5) Key = <T 0 r> Value = occurrences (Sum of instances) - T_r_01
6) Key = <T 1 r> Value = occurrences (Sum of instances) - T_r_10
Output:
1) Key = <r, **> Value = <** N>
2) Key = <r, **> Value = <N 0 N_r_0>
3) Key = <r, **> Value = <N 1 N_r_1>
4) Key = <r, **> Value = <T 0 T_r_01>
5) Key = <r, **> Value = <T 1 T_r_10>
6) Key = <r, w1 w2 w3> Value = <w1 w2 w3>
- 'Reducer' –
Input:
1) Key = <r **> Value = [** occurrences, N 0 occ, N 1 occ, T 0 occ, T 1 occ]
2) Key = <r w1 w2 w3> Value = <w1 w2 w3>
Output:
Key = w1 w2 w3 Value = probability

Output screenshot -
![image](https://github.com/IdanArbiv/Knowledge-base-for-Word-Prediction/assets/101040591/faf3428f-db02-4f8f-8ab2-3294b512de3b)

# Step 4 –
In step 4 we do not make a fundamental change in Mapper and Reducer as you can 
see, but the main goal is to use the compare function for sorting the 3-grams
and the probability as required
- 'Mapper' –
Input:
<w1 w2 w3, probability>
Output:
<w1 w2 w3 probability, "">
- 'Reducer' –
Input:
<w1 w2 w3 probability, "">
Output:
<w1 w2 w3 probability, "">

Output screenshot -
![image](https://github.com/IdanArbiv/Knowledge-base-for-Word-Prediction/assets/101040591/47aec003-4522-4f09-8b1e-dbc92073d6db)

# Scalability
o in the first step, instead of counting the number of occurrences of 
threes in the corpus only, we optimize the counting and also count the 
number of occurrences of each 3-grams in each half of the corpus. We 
don't pay for this at runtime because we go through all the 3-grams
anyway and the number of keys remains the same.
o In the second step instead of matching N to every possible r, we 
identify only the existing r's and match only them using an efficient 
argmax search.
o We will note that every 3-grams that appears r times in the corpus will 
have the same probability of appearing according to the formula. 
Therefore already in the second step for each 3-grams we receive and for 
each r, we will count all the parameters required to calculate the 
probability for each r.
o In the third step we "match" to each r all the parameters required to 
calculate the probability. After that, we will send the required 
parameters for the probability before sending the 3 grams and thus we 
will not put anything on the memory, also, we will make sure that all the 
keys that touch the same r will send to the same reducer.
o In the fourth step, instead of saving the probability results in 
memory, we use the EMR template, particularly the COMPARE method, and 
number the 3 grams and the probabilities in the required order.

# Effective use of cloud services
The project significantly saves on cloud services.
o Because of the design we made that optimizes processes at every stage, 
we save significantly on communication and perform only 4 stages!
As a result, to get a result in a reasonable time, the EMR system 
consumes fewer resources and the user can use fewer instances.
o The system we built makes use of EMR, this system makes many
optimizations, from memory storage to efficient communication of the EC2 
computers it runs.
o The data we read is data that is read sequentially through the S3 
interface, so there is no need to upload it to our bucket.

# Security
o The credentials are not written or hard coded anywhere in the project.
o The credentials are exchanged and saved each time inside the local 
computer under a folder that we protected with a password

# Running Times
o Without local aggregation - 4 hours and 3 minutes
o With local aggregation - 3 hours and 32 minutes.

# Statistics
We will provide the number of key-value pairs that were sent from the mappers to 
the reducers in your map-reduce runs, and their size with and without local 
aggregation.
- Without local aggregation
Step 1: Map outputs records = 4490889434
Reduce input records = 4490889434
Step 2: Map outputs records = 271803885
Reduce input records = 271803885
Step 3: Map outputs records = 45560258
Reduce input records = 45560258
Step 4: Map outputs records = 45276387
Reduce input records = 45276387
- With local aggregation (Using Combiner in steps 1 and 2)
Step 1: Map outputs records = 4490889434
Reduce input records = 90614691
Step 2: Map outputs records = 271803885
Reduce input records = 91468628
Step 3: Map outputs records = 45560258
Reduce input records = 45560258
Step 4: Map outputs records = 45276387
Reduce input records = 45276387

We can see a large difference in network communication between the two 
experiments (with and without local aggregation).
When we used the combiner in the second experiment, there were much fewer keys 
and values that have been sent to the Reducer from the Mapper, meaning that 
there was a significant improvement in the number of communication operations. 
This also affects the runtime differences between the 2 experiments. The first 
experiment without local aggregation lasted 4 hours and 3 minutes compared to 
the second experiment which lasted only 3 hours and 32 minutes!

# Analysis
We will choose 10 'interesting' word pairs and show their top 5 next words. 
We will also judge whether the system got to a reasonable decision for these 
cases.
1) “WHERE ARE”
WHERE ARE THE 1.0134418402158696E-7
WHERE ARE YOU 8.750133492890239E-8
WHERE ARE WE 6.140941331210702E-8
WHERE ARE THEY 3.3018936973699524E-8
WHERE ARE THOSE 3.5688371401945458E-9
“WHERE ARE THOSE” is a less common sentence so we got a reasonable decision for 
these cases.
2) “WHO ARE”
WHO ARE THE 1.8107105435032328E-7
WHO ARE YOU 6.223041034732237E-8
WHO ARE THEY 4.5616503166532846E-8
WHO ARE INTERESTED 1.0763552242313785E-8 
WHO ARE DEAF 7.449068240054423E-9
“WHO ARE DEAF” is a less common sentence so we got a reasonable decision for 
these cases.
3) “What Do”
What Do We 6.54937336308145E-7
What Do They 1.7539608546596144E-7
What Do We 6.54937336308145E-7
What Do Parents 8.194481026709323E-9
What Do Jews 8.117434571197274E-9
“What Do Parents” and “What Do Jews” is less common sentences so we got 
reasonable decisions for these cases.
4) “DO YOU”
DO YOU THINK 4.11380793223241E-7
DO YOU WANT 3.418604104588211E-7
DO YOU KNOW 3.0361583009292923E-7
DO YOU HAVE 1.823373925374581E-7
DO YOU REMEMBER 6.311444666211596E-8
All sentences make sense and we got reasonable decisions for these cases.
5) “I WANT”
I WANT TO 4.11380793223241E-7
I WANT YOU 7.837910456449924E-8
I WANT A 3.62395354255828E-8
I WANT IT 3.084176591282887E-8
I WANT THE 2.493666866275241E-8
All sentences make sense and we got a reasonable decision for these cases.
6) “I WAS”
I WAS BORN 8.614084003250145E-8
I WAS IN 7.700382038451697E-8
I WAS THERE 2.7780807382669693E-8
I WAS THE 2.6944099864213536E-8
I WAS AN 2.681385711773884E-8
All sentences make sense and we got a reasonable decision for these cases.
7) “IT CAN”
IT CAN BE 1.2810694134267506E-7
IT CAN NEVER 1.1515666099606278E-8
IT CAN HAPPEN1.924961099271618E-8
IT CAN ONLY 2.8069785548968477E-9
IT CAN HELP 1.3054779935853503E-9
The first three sentences make sense but the last two are less common so we got a 
reasonable decision for these cases.
8) “IT CAN”
IT IS CLEAR 1.4937252069182754E-8
IT IS HARD 1.4730582107940129E-8
IT IS BEST 1.1878892309523708E-8
IT IS BASED 1.1515666099606278E-8
IT IS EXTREMELY 1.1515666099606278E-8
All sentences make sense and we got reasonable decisions for these cases.
9) “IT MEANS”
IT MEANS THAT1.42134686081324E-8
IT MEANS TODAY 5.490839194237495E-9
IT MEANS FOR 5.078416645819533E-9
IT CAN ONLY 2.8069785548968477E-9
IT MEANS. 4.801722785466548E-9
The first sentence makes sense but the rest are not so we got reasonable decisions
for these cases.
10) “If You”
If You Got 1.5440376629378523E-8
If You Work 1.616597864472744E-8
If You Say 1.2904578034383496E-8
If You Would 1.2311436979109751E-8
If You Will 1.559930610476532E-8
All sentences make sense and we got a reasonable decision for these cases.

# Conclusions
The running of the program can be further improved according to the 
requirements of the program, in this program we did not assume that 
almost anything can be stored in memory, but in real programs, it may be 
that the program we want to run will work with data that has a certain 
limit and therefore it is possible to store certain parameters in memory 
and significantly optimize the running time on by reducing the running 
time needed for communication




