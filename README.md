# mongo-network
A simple testbed for network-type performance measurements in MongoDB

This is a quickly hacked tool that creates a scale-free network of three types of nodes, items, groups and users that are connected by two types of edges: users that follow groups (followers) and groups that group items (ItemGroup).

The idea is to find out whether, for reasonably sized networks, a *read-heavy* approach is still suitable for MongoDB, or whether a more write-heavy approach is required.

This was inspired by a [question on StackOverflow][http://stackoverflow.com/questions/28421505/followers-mongodb-database-design].

Caveats
---

The code is quickly hacked together and is neither optimized, nor free of selection biases and potentially 'weak' random number generators that create patterns in the data - however, I'm quite confident it servers the purposes and, through the existence of supernodes, allows a much more realistic measurement than those made with naive approaches, random networks or uniform distributions.

Also, please note that the code isn't multithreaded nor does it make use of the async capabilities of the MongoDB beta driver.

Lastly, the code has references that it doesn't need and it wasn't tested with Mono - improvements welcome. Enjoy!

