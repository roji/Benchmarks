using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;

BenchmarkRunner.Run<Benchmark>();

[MemoryDiagnoser]
public class Benchmark
{
    private IDbContextFactory<BlogContext> _poolingFactory;
    private BlogContext _context;

    private static readonly Func<BlogContext, IAsyncEnumerable<Blog>> _query
        = EF.CompileAsyncQuery((BlogContext context) => context.Blogs);

    [GlobalSetup]
    public async Task Setup()
    {
        var services = new ServiceCollection();
        services.AddPooledDbContextFactory<BlogContext>(options => options
            .UseNpgsql("Host=localhost;Username=test;Password=test")
            .UseQueryTrackingBehavior(QueryTrackingBehavior.NoTracking)
            .EnableThreadSafetyChecks(false)
        );
        var serviceProvider = services.BuildServiceProvider();
        _poolingFactory = serviceProvider.GetRequiredService<IDbContextFactory<BlogContext>>();

        using (var context = _poolingFactory.CreateDbContext())
        {
            await context.Database.EnsureDeletedAsync();
            await context.Database.EnsureCreatedAsync();
        }

        _context = _poolingFactory.CreateDbContext();
    }

    // [Benchmark]
    public async Task<int> QueryWithPooling()
    {
        using var ctx = _poolingFactory.CreateDbContext();

        var sum = 0;
        await foreach (var blog in _query(ctx))
        {
            sum += blog.Id;
        }

        return sum;
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
        public BlogContext(DbContextOptions options) : base(options) {}

        public DbSet<Blog> Blogs { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
            => modelBuilder.Entity<Blog>().HasData(new Blog { Id = 1, Title = "EF Core perf is awesome" });
    }

    public class Blog
    {
        public int Id { get; set; }
        public string Title { get; set; }
    }
}
