/* The MIT License (MIT)

Copyright (c) 2015 Christoph Menge

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE. */
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace group_followers
{
    public class Item
    {
        public ObjectId Id { get; set; }
        public string Name { get; set; }
    }

    public class Group
    {
        public ObjectId Id { get; set; }
        public string Name { get; set; }

        public double Weight { get; set; }
    }

    public class User
    {
        public ObjectId Id { get; set; }
        public string Name { get; set; }
    }


    public class ItemGroup
    {
        public ObjectId Id { get; set; }
        public ObjectId ItemId { get; set; }
        public ObjectId GroupId { get; set; }
    }

    public class Follower
    {
        public ObjectId Id { get; set; }
        public ObjectId UserId { get; set; }
        public ObjectId GroupId { get; set; }
    }

    public class QueryResult
    {
        public ObjectId UserId { get; set; }
        public int Milliseconds { get; set; }
        public int GroupCount { get; set; }
        public int MatchCount { get; set; }
    }

    class Program
    {
        const int BATCH_SIZE = 1000;
        
        const int USER_COUNT = 50000;
        const int ITEM_COUNT = 200000;
        const int GROUP_COUNT = 20000;
        
        //const int USER_COUNT = 5000;
        //const int ITEM_COUNT = 20000;
        //const int GROUP_COUNT = 2000;

        static Dictionary<ObjectId, User> users = new Dictionary<ObjectId, User>();
        static Dictionary<ObjectId, Item> items = new Dictionary<ObjectId, Item>();
        static Dictionary<ObjectId, Group> groups = new Dictionary<ObjectId, Group>();

        static long groupItemLinks = 0;
        static long userGroupLinks = 0;

        static void WriteUsers(IMongoCollection<User> userCollection)
        {
            List<User> batchList = new List<User>();
            for (int i = 0; i < USER_COUNT; i++)
            {
                var user = new User { Name = "User " + i, Id = ObjectId.GenerateNewId() };
                batchList.Add(user);
                users.Add(user.Id, user);
                if (i > 0 && i % BATCH_SIZE == 0)
                {
                    userCollection.InsertManyAsync(batchList).Wait();
                    batchList.Clear(); // async ok?
                    
                }
            }
            userCollection.InsertManyAsync(batchList);
        }

        static void WriteItems(IMongoCollection<Item> itemCollection)
        {
            List<Item> batchList = new List<Item>();
            for (int i = 0; i < ITEM_COUNT; i++)
            {
                var item = new Item { Name = "Item " + i, Id = ObjectId.GenerateNewId() };
                batchList.Add(item);
                items.Add(item.Id, item);
                if (i > 0 && i % BATCH_SIZE == 0)
                {
                    itemCollection.InsertManyAsync(batchList).Wait();
                    batchList.Clear(); // async ok?
                    
                }
            }
            itemCollection.InsertManyAsync(batchList);
        }

        static void WriteGroups(IMongoCollection<Group> groupCollection)
        {
            var dist = new Troschuetz.Random.Distributions.Continuous.BetaDistribution(1.6, 20.0);

            var batchList = new List<Group>();
            for (int i = 0; i < GROUP_COUNT; i++)
            {
                var group = new Group { Name = "Group " + i, Id = ObjectId.GenerateNewId(), Weight = dist.NextDouble() };
                batchList.Add(group);
                groups.Add(group.Id, group);
                if (i > 0 && i % BATCH_SIZE == 0)
                {
                    groupCollection.InsertManyAsync(batchList).Wait();
                    batchList.Clear();
                }
            }
            groupCollection.InsertManyAsync(batchList);
        }


        static void ConnectGroupItems(IMongoCollection<ItemGroup> collection)
        {
            // A beta distribution with a very long tail
            var dist = new Troschuetz.Random.Distributions.Continuous.BetaDistribution(1.4, 25.0);
            var rnd = new Random(645);

            List<ItemGroup> batchList = new List<ItemGroup>();

            foreach(var rover in items)
            {
                // Number of groups per item: This is probably beta-distributed
                var numberOfGroups = Math.Min(dist.NextDouble() * 900, GROUP_COUNT);

                // The number of items per group might be beta-distributed as well, but that requires a much smarter
                // shuffling I'm afraid
                var groupsShuffled = groups.OrderBy((item) => rnd.Next() * item.Value.Weight);

                int counter = 0;
                foreach(var groupRover in groupsShuffled)
                {
                    if (counter++ > numberOfGroups)
                        break;

                    var itemGroup = new ItemGroup { GroupId = groupRover.Key, ItemId = rover.Key };
                    batchList.Add(itemGroup);

                    groupItemLinks++;

                    if (batchList.Count % BATCH_SIZE == 0)
                    {
                        collection.InsertManyAsync(batchList).Wait();
                        batchList.Clear();
                    }
                }
            }

            collection.InsertManyAsync(batchList);
        }

        static void ConnectFollowers(IMongoCollection<Follower> collection)
        {
            // For each item, add it to a number of groups. The number of groups per item should be beta-distributed
            var dist = new Troschuetz.Random.Distributions.Continuous.BetaDistribution(1.4, 25.0);
            var rnd = new Random(5234);

            List<Follower> batchList = new List<Follower>();

            foreach (var rover in users)
            {
                var numberOfGroups = Math.Min(dist.NextDouble() * 750, GROUP_COUNT);
                var groupsShuffled = groups.OrderBy((item) => rnd.Next());

                int counter = 0;
                foreach (var groupRover in groupsShuffled)
                {
                    if (counter++ > numberOfGroups)
                        break;

                    var follower = new Follower { GroupId = groupRover.Key, UserId = rover.Key };
                    batchList.Add(follower);

                    userGroupLinks++;

                    if (batchList.Count % BATCH_SIZE == 0)
                    {
                        collection.InsertManyAsync(batchList).Wait();
                        batchList.Clear();
                    }
                }
            }

            collection.InsertManyAsync(batchList);
        }

        static void Main(string[] args)
        {
            bool write = false;

            Stopwatch sw = new Stopwatch();
                        
            MongoClient client = new MongoClient("mongodb://localhost:27017/");
            var db = client.GetDatabase("followers");

            var userCollection = db.GetCollection<User>("User");
            var itemCollection = db.GetCollection<Item>("Item");
            var itemGroupCollection = db.GetCollection<ItemGroup>("ItemGroup");
            var followersCollection = db.GetCollection<Follower>("Follower");
            var groupCollection = db.GetCollection<Group>("Group");


            if (write)
            {
                Console.WriteLine("Inserting Users...");
                WriteUsers(userCollection);
                Console.WriteLine("Inserting Items...");
                WriteItems(itemCollection);
                Console.WriteLine("Inserting Groups...");
                WriteGroups(groupCollection);
                Console.WriteLine("Connecting Groups / Items...");
                ConnectGroupItems(itemGroupCollection);
                Console.WriteLine("Connecting Groups / Users...");
                ConnectFollowers(followersCollection);

                System.Console.WriteLine("Item/Group links: {0} ", groupItemLinks);
                System.Console.WriteLine("User/Group links: {0} ", userGroupLinks);

                Console.WriteLine("Creating Indexes...");
                itemGroupCollection.IndexManager.CreateIndexAsync(IndexKeys<ItemGroup>.Ascending(p => p.ItemId, p => p.GroupId), new CreateIndexOptions() { Background = true, Unique = true }).Wait();
                followersCollection.IndexManager.CreateIndexAsync(IndexKeys<Follower>.Ascending(p => p.UserId), new CreateIndexOptions() { Background = true }).Wait();
            }
            else
            {
                // I think with the following design a real performance issue could be when I want 
                // to get all of the groups that a user is following for a specific item (based off
                // of the user_id and item_id), because then I have to find all of the groups the
                // user is following, and from that find all of the item_groups with the group_id
                // $in and the item id. (but I can't actually see any other way of doing this)

                // get the groups of a somewhat random user

                List<long> times = new List<long>();
                List<QueryResult> results = new List<QueryResult>();

                bool sampleRandomUsers = true;

                if (sampleRandomUsers)
                {
                    var rnd = new Random(346);

                    for (int i = 0; i < 10000; i++)
                    {
                        sw.Start();
                        var user = userCollection.Find(p => true).Skip(rnd.Next() % USER_COUNT).Limit(1).FirstAsync().Result;
                        var itemIndex = rnd.Next() % ITEM_COUNT;
                        var item = itemCollection.Find(p => true).Skip(itemIndex).Limit(1).FirstAsync().Result;
                        sw.Stop();


                        sw.Restart();
                        var groupsOfUser = followersCollection.Find(Query<Follower>.EQ(p => p.UserId, user.Id));
                        var groupsToQuery = groupsOfUser.Projection(p => p.GroupId).ToListAsync().Result;
                        var itemsFollowed = itemGroupCollection.Find(Query.And(Query<ItemGroup>.EQ(p => p.ItemId, item.Id), Query<ItemGroup>.In(p => p.GroupId, groupsToQuery)));
                        sw.Stop();

                        var itemCount = itemsFollowed.ToListAsync().Result.Count;
                        // var itemCount = itemsFollowed.CountAsync().Result;


                        times.Add(sw.ElapsedMilliseconds);
                        results.Add(new QueryResult
                        {
                            GroupCount = groupsToQuery.Count,
                            MatchCount = (int)itemCount,
                            Milliseconds = (int)sw.ElapsedMilliseconds,
                            UserId = user.Id
                        });
                    }
                }
                else
                {
                    // HACK! Hardcoded ids of supernodes...
                    
                    // super item: 
                    int counter = 0;
                    var item = itemCollection.Find(p => p.Id == new ObjectId("54e488640917b62270ebeeff")).FirstAsync().Result;

                    var superUsers = followersCollection.Find(Query<Follower>.EQ(p => p.GroupId, new ObjectId("54e488670917b62270ed287c"))).ToListAsync().Result;

                    foreach (var user in superUsers)
                    {
                        if (counter > 50)
                            break;

                        sw.Restart();
                        var groupsOfUser = followersCollection.Find(Query<Follower>.EQ(p => p.UserId, user.UserId));
                        var groupsToQuery = groupsOfUser.Projection(p => p.GroupId).ToListAsync().Result;
                        var itemsFollowed = itemGroupCollection.Find(Query.And(Query<ItemGroup>.EQ(p => p.ItemId, item.Id), Query<ItemGroup>.In(p => p.GroupId, groupsToQuery))).ToListAsync().Result;
                        sw.Stop();

                        var itemCount = itemsFollowed.Count;

                        times.Add(sw.ElapsedMilliseconds);
                        results.Add(new QueryResult
                        {
                            GroupCount = groupsToQuery.Count,
                            MatchCount = (int)itemCount,
                            Milliseconds = (int)sw.ElapsedMilliseconds,
                            UserId = user.Id
                        });
                    }
                }

                Console.WriteLine("Fastest: {0}ms, slowest: {1}ms", times.Min(), times.Max());
            }
        }
    }
}
