using Amazon.S3;
using S3ExportApi.ApproachA_StreamingZip;
using S3ExportApi.ApproachB_StagedZip;
using S3ExportApi.Configuration;
using S3ExportApi.Storage;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<S3Options>(builder.Configuration.GetSection("S3"));
builder.Services.AddDefaultAWSOptions(builder.Configuration.GetAWSOptions());
builder.Services.AddAWSService<IAmazonS3>();

builder.Services.AddSingleton<IExportRepository, InMemoryExportRepository>();

// Approach A
builder.Services.AddSingleton<ExportSessionStore>();

// Approach B — async zip on /complete, drained by ExportZipJobWorker
builder.Services.AddSingleton<ExportZipJobQueue>();
builder.Services.AddScoped<ExportZipJob>();
builder.Services.AddHostedService<ExportZipJobWorker>();

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();
app.UseSwagger();
app.UseSwaggerUI();
app.MapControllers();
app.Run();
