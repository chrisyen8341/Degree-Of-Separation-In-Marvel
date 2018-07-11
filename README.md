# Degree-Of-Separation-In-Marvel
This is a spark program written in Java to analyze(Use BFS Algrorithm) Degree of Separation between Marvel Hero(
e.g How many people between Iron Man and "Invisible Woman")

Environment Info:::
Spark version:2.1.1
Java JDK:1.8
IDE:Eclipse and Maven

Data Structure:::
File Marvel-Graph,the first column means HeroId,and the Id aftet first coulmn means the hero's friend
(e.g 100 2 3 5 11  means HeroId 100 has friend 2 3 5 11)


Brief Program structure:::
1.We use Breath First Search algorithm to find shootest path.Each hero stand as a node. And there is a startnode(e,g IRON MAN)
and targetnode(e.g "Invisible Woman")
2.We use Color as a flag: WHITE(Unprocess node) GRAY(Ready to process node) BLACK(Processed node), and we use variable distance stand for 
distance from each node to stated node,and unprocessed node Distance we set to 9999 

Steps:::
0.Get Started RDD,turn each line into Tuple (HeroId,(Connection,Distance,Color)),and the StartedId Color to gray
1.For loop, we assume we can run all of it with 10 Degree(A lot,mean is less than 6)
2.Map phase:
2-1.Process Gray Node,and create new nodes for each gray node in connection with distance increment by 1,without any connection,
2-2.Make node we just the proccesed color to black. 
3.Reduce phase:You can tell from map, it will have duplicate key values, so we need to reduce by key to combine from same hero.
4.Keep for loop until find it.



Note:::
This Java version is completely written by me, this is very important, beacuse it will contain many Error.Hahaha!
Really recommend beginner like me to go to Udemy Taming Big Data with Apache Spark and Python, the original python version 
is taught by this course

