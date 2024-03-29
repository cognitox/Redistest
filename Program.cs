﻿using Microsoft.Extensions.Configuration;
using Pipelines.Sockets.Unofficial.Arenas;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Redistest
{
    class Employee
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public int Age { get; set; }

        public Employee(string id, string name, int age)
        {
            Id = id;
            Name = name;
            Age = age;
        }
    }

    class Program
    {
        private static RedisConnection _redisConnection;

        static async Task Main(string[] args)
        {
            // Initialize
            var builder = new ConfigurationBuilder()
                .AddUserSecrets<Program>()
                .AddJsonFile($"config.json", false, true);
                
            var configuration = builder.Build();
            CacheConfig cacheConfig = configuration.GetSection("CacheConfig").Get<CacheConfig>();

            _redisConnection = await RedisConnection.InitializeAsync(connectionString: $"{cacheConfig.Name}.redis.cache.windows.net,abortConnect=false,ssl=true,allowAdmin=true,password={cacheConfig.Password}");

            try
            {
                // Perform cache operations using the cache object...
                Console.WriteLine("Running... Press any key to quit.");

                //Set Keys first line to set keys first.
                if (cacheConfig.SetKeysFirst)
                {
                    Task thread1 = Task.Run(() => SetKeys("Thread 1", cacheConfig.KeyPrefix, cacheConfig.NumberOfKeysToSet));
                    Task.WaitAll(thread1);
                }
                if (cacheConfig.ClusterMode)
                {
                    while (!Console.KeyAvailable)
                    {
                        Task thread2 = Task.Run(() => GetKeys(cacheConfig.KeyPrefix));
                        Thread.Sleep(5000);
                        Task.WaitAll(thread2);
                    }
                }
                else
                {
                    Task thread1 = Task.Run(() => RunRedisCommandsAsync("Thread 1"));
                    Task thread2 = Task.Run(() => RunRedisCommandsAsync("Thread 2"));
                    Thread.Sleep(5000);
                    Task.WaitAll(thread1, thread2);                    
                }
                
            }
            finally
            {
                _redisConnection.Dispose();
            }
        }

        private static async Task RunRedisCommandsAsync(string prefix)
        {

            // Simple PING command
            Console.WriteLine($"{Environment.NewLine}{prefix}: Cache command: PING");
            RedisResult pingResult = await _redisConnection.BasicRetryAsync(async (db) => await db.ExecuteAsync("PING"));
            Console.WriteLine($"{prefix}: Cache response: {pingResult}");

            // Simple get and put of integral data types into the cache
            string key = "Message";
            string value = "Hello! The cache is working from a .NET Core console app!";

            Console.WriteLine($"{Environment.NewLine}{prefix}: Cache command: GET {key} via StringGetAsync()");
            RedisValue getMessageResult = await _redisConnection.BasicRetryAsync(async (db) => await db.StringGetAsync(key));
            Console.WriteLine($"{prefix}: Cache response: {getMessageResult}");

            Console.WriteLine($"{Environment.NewLine}{prefix}: Cache command: SET {key} \"{value}\" via StringSetAsync()");
            bool stringSetResult = await _redisConnection.BasicRetryAsync(async (db) => await db.StringSetAsync(key, value));
            Console.WriteLine($"{prefix}: Cache response: {stringSetResult}");

            Console.WriteLine($"{Environment.NewLine}{prefix}: Cache command: GET {key} via StringGetAsync()");
            getMessageResult = await _redisConnection.BasicRetryAsync(async (db) => await db.StringGetAsync(key));
            Console.WriteLine($"{prefix}: Cache response: {getMessageResult}");

            // Store serialized object to cache
            Employee e007 = new Employee("007", "Davide Columbo", 100);
            stringSetResult = await _redisConnection.BasicRetryAsync(async (db) => await db.StringSetAsync("e007", JsonSerializer.Serialize(e007)));
            Console.WriteLine($"{Environment.NewLine}{prefix}: Cache response from storing serialized Employee object: {stringSetResult}");

            // Retrieve serialized object from cache
            getMessageResult = await _redisConnection.BasicRetryAsync(async (db) => await db.StringGetAsync("e007"));
            Employee e007FromCache = JsonSerializer.Deserialize<Employee>(getMessageResult.ToString());
            Console.WriteLine($"{prefix}: Deserialized Employee .NET object:{Environment.NewLine}");
            Console.WriteLine($"{prefix}: Employee.Name : {e007FromCache.Name}");
            Console.WriteLine($"{prefix}: Employee.Id   : {e007FromCache.Id}");
            Console.WriteLine($"{prefix}: Employee.Age  : {e007FromCache.Age}{Environment.NewLine}");
        }

        private static async Task SetKeys(string threadId, string keyPrefix, int numberOfKeysToSet = 200)
        {
            string key = "";
            string value = "";

            for (int i = 0; i < numberOfKeysToSet; i++)
            {
                // Simple get and put of integral data types into the cache
                 key = $"{keyPrefix}{i}";
                 value = $"Hello! This is value for {key}";
                Console.WriteLine($"{Environment.NewLine}{threadId}: Cache command: SET {key} \"{value}\" via StringSetAsync()");
                bool stringSetResult = await _redisConnection.BasicRetryAsync(async (db) => await db.StringSetAsync(key, value));
                Console.WriteLine($"{threadId}: Cache response: {stringSetResult}");

            }
        }
        /// <summary>
        /// https://stackexchange.github.io/StackExchange.Redis/KeysScan
        /// </summary>
        /// <param name="pattern">Get by pattern - only available in cluster mode</param>
        /// <returns></returns>
        private static async Task GetKeys(string pattern)
        {            
            // Get All keys
            Console.WriteLine($"{Environment.NewLine}{pattern}: Cache command: keys *pattern*");

            var endpoints = _redisConnection.GetEndpoints();
            var _conn = _redisConnection.GetConnection();

            
            Console.WriteLine($"{Environment.NewLine}Total nodes in the Redis cluser is: Cache command: keys {endpoints.Count}");
            foreach (var endpoint in endpoints)
            {
                Console.WriteLine($"List of nodes are as follows:");
                Console.WriteLine($"{Environment.NewLine}{endpoint}");
            }
            foreach (var endpoint in endpoints)
            {
                var point = endpoint.ToString();
                var server = _conn.GetServer(endpoint.ToString());
                Console.WriteLine($"Current Shard is {endpoint}");
                foreach (var key in server.Keys(pattern: $"*{pattern}*"))
                {
                    Console.WriteLine(key);
                }
            }
        }
        
    }
}