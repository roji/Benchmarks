using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

BenchmarkRunner.Run<Benchmark>();

public class Benchmark
{
    private PooledDbContextFactory<BlogContext> _contextFactory;

    [GlobalSetup]
    public async Task Setup()
    {
        var builder = new DbContextOptionsBuilder<BlogContext>();
        builder.UseNpgsql("Host=localhost;Username=test;Password=test");
        _contextFactory = new PooledDbContextFactory<BlogContext>(builder.Options);

        using var ctx = _contextFactory.CreateDbContext();
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
    }

    [Benchmark]
    public async Task Foo()
    {
        using var ctx = _contextFactory.CreateDbContext();

        _ = await ctx.Blogs.ToListAsync();
    }
}

public class BlogContext : DbContext
{
    public DbSet<Blog> Blogs { get; set; }

    public BlogContext(DbContextOptions options) : base(options) {}
}

public class Blog
{
    public int Id { get; set; }
    public string Name { get; set; }
}
