<h1> Assignment</h1>
<h2> How to build Assignment </h2>
<code> sbt assembly </code>

<h2> How to run Assignment </h2>
 <code>
   ./spark-submit  --class com.newday.Launcher --master local[*]  ${path to assignment-1.0.jar} ${path to properties file}
</code>

<h2> Supported properties </h2>
<table style="width:100%">
  <tr> <th>Property Name</th> <th>Description</th> </tr>
  <tr> <td> movies.input.file</td><td>Path of the input movie file</td> </tr>
   <tr> <td> movies.read.format</td><td>format of the input movie file</td> </tr>

  <tr> <td> movies.output.file</td><td>Write path of the tranformed movie file</td> </tr>
   <tr> <td> movies.write.format</td><td>format of the transformed movie file</td> </tr>
  
  
  <tr> <td> ratings.input.file</td><td>Path of the input movie file</td> </tr>
   <tr> <td> ratings.read.format</td><td>format of the input movie file</td> </tr>

  <tr> <td> ratings.output.file</td><td>Write path of the tranformed ratings file</td> </tr>
   <tr> <td> ratings.write.format</td><td>format of the transformed ratings file</td> </tr>
  
  
  <tr> <td> movie_ratings.output.file</td><td>Write path of the tranformed movie_rating file</td> </tr>
   <tr> <td> movie_ratings.write.format</td><td>format of the transformed movie_rating file</td> </tr>
</table>
