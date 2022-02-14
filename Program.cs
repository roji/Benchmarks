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
        builder.UseInMemoryDatabase("foo");
        // builder.UseNpgsql("Host=localhost;Username=test;Password=test");
        _contextFactory = new PooledDbContextFactory<BlogContext>(builder.Options);

        using var ctx = _contextFactory.CreateDbContext();
        await ctx.Database.EnsureDeletedAsync();
        await ctx.Database.EnsureCreatedAsync();
    }

    [Benchmark]
    public async Task QueryAndTrack()
    {
        using var ctx = _contextFactory.CreateDbContext();

        var blog = await ctx.Blogs.SingleAsync();
        unchecked
        {
            blog.Foo++;
        }

        await ctx.SaveChangesAsync();
    }
}

public class BlogContext : DbContext
{
    public DbSet<Blog> Blogs { get; set; }

    public BlogContext(DbContextOptions options) : base(options) {}

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Blog>().HasData(new Blog { Id = 1, Foo = 1 });
    }
}

public class Blog
{
    public int Id { get; set; }
    public int Foo { get; set; }
}
