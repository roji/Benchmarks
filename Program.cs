using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Microsoft.EntityFrameworkCore;

BenchmarkRunner.Run<Benchmark>();

[MemoryDiagnoser]
public class Benchmark
{
    private BlogContext _context;

    private static readonly Func<BlogContext, IAsyncEnumerable<Blog>> _query
        = EF.CompileAsyncQuery((BlogContext context) => context.Blogs);

    [GlobalSetup]
    public async Task Setup()
    {
        using (var context = new BlogContext())
        {
            await context.Database.EnsureDeletedAsync();
            await context.Database.EnsureCreatedAsync();
        }

        _context = new BlogContext();
    }

    [Benchmark]
    public async Task<int> QueryWithoutPooling()
    {
        var sum = 0;
        await foreach (var blog in _query(_context))
        {
            sum += blog.Id;
        }

        return sum;
    }

    public class BlogContext : DbContext
    {
        public DbSet<Blog> Blogs { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
            => optionsBuilder.UseNpgsql("Host=localhost;Username=test;Password=test")
                .UseQueryTrackingBehavior(QueryTrackingBehavior.NoTracking)
                .EnableThreadSafetyChecks(false); // Comment out on EF Core 5.0

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<Blog>().HasData(new Blog { Id = 1, Title = "EF Core perf is awesome" });
    }

    public class Blog
    {
        public int Id { get; set; }
        public string Title { get; set; }
    }
}
