using Microsoft.Extensions.Configuration;
using Npgsql;
using System.Data;

namespace DatabaseCleanup
{
    public class Program
    {
        private static readonly string LogFileName = $"cleanup_log_{DateTime.Now:yyyyMMdd_HHmmss}.txt";
        private static StreamWriter logWriter;
        private static IConfiguration configuration;

        public static async Task Main(string[] args)
        {
            try
            {
                InitializeLogging();
                InitializeConfiguration();
                await LogAsync("=== Database Cleanup Process Started ===");
                await LogAsync($"Timestamp: {DateTime.Now}");

                await ProcessDuplicateCleanup();

                await LogAsync("=== Database Cleanup Process Completed Successfully ===");
            }
            catch (Exception ex)
            {
                await LogAsync($"FATAL ERROR: {ex.Message}");
                await LogAsync($"Stack Trace: {ex.StackTrace}");
                Console.WriteLine($"Error: {ex.Message}");
                Console.WriteLine("Check log file for details.");
            }
            finally
            {
                logWriter?.Dispose();
                Console.WriteLine($"Log saved to: {LogFileName}");
                Console.WriteLine("Press any key to exit...");
                Console.ReadKey();
            }
        }

        private static void InitializeConfiguration()
        {
            string basePath = Path.Combine(AppContext.BaseDirectory, "..", "..", "..");

            var builder = new ConfigurationBuilder()
                .SetBasePath(basePath)
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true);

            configuration = builder.Build();

            var iamConnectionString = configuration.GetConnectionString("IAMConnection");
            var sdgConnectionString = configuration.GetConnectionString("SDGConnection");

            if (string.IsNullOrEmpty(iamConnectionString))
                throw new InvalidOperationException("IamDatabase connection string not found in appsettings.json");

            if (string.IsNullOrEmpty(sdgConnectionString))
                throw new InvalidOperationException("SdgDatabase connection string not found in appsettings.json");
        }

        private static void InitializeLogging()
        {
            logWriter = new StreamWriter(LogFileName, true);
            logWriter.AutoFlush = true;
        }

        private static async Task LogAsync(string message)
        {
            var logMessage = $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {message}";
            Console.WriteLine(logMessage);
            await logWriter.WriteLineAsync(logMessage);
        }

        private static async Task ProcessDuplicateCleanup()
        {
            var iamConnectionString = configuration.GetConnectionString("IAMConnection");
            var sdgConnectionString = configuration.GetConnectionString("SDGConnection");

            using var iamConnection = new NpgsqlConnection(iamConnectionString);
            using var sdgConnection = new NpgsqlConnection(sdgConnectionString);

            await iamConnection.OpenAsync();
            await sdgConnection.OpenAsync();

            await LogAsync("Database connections established");

            // Start transactions
            using var iamTransaction = await iamConnection.BeginTransactionAsync();
            using var sdgTransaction = await sdgConnection.BeginTransactionAsync();

            try
            {
                await LogAsync("Transactions started");

                // Step 1: Find duplicate emails in AspNetUsers
                var duplicateEmails = await FindDuplicateEmails(iamConnection, iamTransaction);
                await LogAsync($"Found {duplicateEmails.Count} emails with duplicates");

                if (duplicateEmails.Count == 0)
                {
                    await LogAsync("No duplicate emails found. Nothing to clean up.");
                    return;
                }

                // Step 2: Process each duplicate email group
                int totalProcessed = 0;
                int totalUsersDeleted = 0;
                int totalAspNetUsersDeleted = 0;
                int totalUserAgenciesDeleted = 0;
                int totalUserDivisionsDeleted = 0;
                int totalUserSectionsDeleted = 0;

                foreach (var email in duplicateEmails.Keys)
                {
                    await LogAsync($"\n--- Processing email: {email} ---");
                    var userIds = duplicateEmails[email];
                    await LogAsync($"Found {userIds.Count} AspNetUsers records with this email: {string.Join(", ", userIds)}");

                    // Find corresponding Users records
                    var usersData = await FindUsersForAspNetUserIds(sdgConnection, sdgTransaction, userIds);
                    await LogAsync($"Found {usersData.Count} corresponding Users records");

                    if (usersData.Count == 0)
                    {
                        await LogAsync("No Users records found for these AspNetUsers. Skipping...");
                        continue;
                    }

                    // Find the Users record with the smallest Id (first one)
                    var keepUser = usersData.OrderBy(u => u.UsersId).First();
                    var deleteUsers = usersData.Where(u => u.UsersId != keepUser.UsersId).ToList();

                    await LogAsync($"Keeping Users record: Id={keepUser.UsersId}, Sub={keepUser.Sub}");

                    // Delete UserAgencies, UserDivisions, and UserSections records for Users that will be deleted
                    foreach (var deleteUser in deleteUsers)
                    {
                        await LogAsync($"Deleting UserAgencies records for UserId={deleteUser.UsersId}");
                        var deletedUserAgencies = await DeleteUserAgenciesRecords(sdgConnection, sdgTransaction, deleteUser.UsersId);
                        totalUserAgenciesDeleted += deletedUserAgencies;

                        await LogAsync($"Deleting UserDivisions records for UserId={deleteUser.UsersId}");
                        var deletedUserDivisions = await DeleteUserDivisionsRecords(sdgConnection, sdgTransaction, deleteUser.UsersId);
                        totalUserDivisionsDeleted += deletedUserDivisions;

                        await LogAsync($"Deleting UserSections records for UserId={deleteUser.UsersId}");
                        var deletedUserSections = await DeleteUserSectionsRecords(sdgConnection, sdgTransaction, deleteUser.UsersId);
                        totalUserSectionsDeleted += deletedUserSections;
                    }

                    // Delete other Users records
                    foreach (var deleteUser in deleteUsers)
                    {
                        await LogAsync($"Deleting Users record: Id={deleteUser.UsersId}, Sub={deleteUser.Sub}");
                        await DeleteUsersRecord(sdgConnection, sdgTransaction, deleteUser.UsersId);
                        totalUsersDeleted++;
                    }

                    // Delete AspNetUsers records that don't match the kept Sub
                    var aspNetUsersToDelete = userIds.Where(id => id != keepUser.Sub).ToList();
                    foreach (var aspNetUserId in aspNetUsersToDelete)
                    {
                        await LogAsync($"Deleting AspNetUsers record: Id={aspNetUserId}");
                        await DeleteAspNetUsersRecord(iamConnection, iamTransaction, aspNetUserId);
                        totalAspNetUsersDeleted++;
                    }

                    await LogAsync($"Kept AspNetUsers record: Id={keepUser.Sub}");
                    totalProcessed++;
                }

                await LogAsync($"\n=== SUMMARY ===");
                await LogAsync($"Total email groups processed: {totalProcessed}");
                await LogAsync($"Total UserAgencies records deleted: {totalUserAgenciesDeleted}");
                await LogAsync($"Total UserDivisions records deleted: {totalUserDivisionsDeleted}");
                await LogAsync($"Total UserSections records deleted: {totalUserSectionsDeleted}");
                await LogAsync($"Total Users records deleted: {totalUsersDeleted}");
                await LogAsync($"Total AspNetUsers records deleted: {totalAspNetUsersDeleted}");

                // Commit transactions
                await iamTransaction.CommitAsync();
                await sdgTransaction.CommitAsync();
                await LogAsync("Transactions committed successfully");
            }
            catch (Exception ex)
            {
                await LogAsync($"Error during processing: {ex.Message}");
                await LogAsync("Rolling back transactions...");

                try
                {
                    await iamTransaction.RollbackAsync();
                    await sdgTransaction.RollbackAsync();
                    await LogAsync("Transactions rolled back successfully");
                }
                catch (Exception rollbackEx)
                {
                    await LogAsync($"Error during rollback: {rollbackEx.Message}");
                }
                throw;
            }
        }

        private static async Task<Dictionary<string, List<Guid>>> FindDuplicateEmails(
            NpgsqlConnection connection, NpgsqlTransaction transaction)
        {
            var duplicates = new Dictionary<string, List<Guid>>();

            const string query = @"
                SELECT ""Email"", ""Id""
                FROM public.""AspNetUsers""
                WHERE ""Email"" IS NOT NULL AND ""Email"" != ''
                ORDER BY ""Email"", ""Id""";

            using var command = new NpgsqlCommand(query, connection, transaction);
            using var reader = await command.ExecuteReaderAsync();

            var emailGroups = new Dictionary<string, List<Guid>>();

            while (await reader.ReadAsync())
            {
                var email = reader.GetString("Email");
                var id = reader.GetGuid("Id");

                if (!emailGroups.ContainsKey(email))
                    emailGroups[email] = new List<Guid>();

                emailGroups[email].Add(id);
            }

            // Filter only emails with duplicates
            foreach (var kvp in emailGroups.Where(g => g.Value.Count > 1))
            {
                duplicates[kvp.Key] = kvp.Value;
                await LogAsync($"Duplicate email '{kvp.Key}': {kvp.Value.Count} records");
            }

            return duplicates;
        }

        private static async Task<List<(int UsersId, Guid Sub)>> FindUsersForAspNetUserIds(
            NpgsqlConnection connection, NpgsqlTransaction transaction, List<Guid> aspNetUserIds)
        {
            var users = new List<(int UsersId, Guid Sub)>();

            if (aspNetUserIds.Count == 0) return users;

            var parameters = string.Join(",", aspNetUserIds.Select((id, index) => $"@param{index}"));
            var query = $@"
                SELECT ""Id"", ""Sub""
                FROM public.""Users""
                WHERE ""Sub"" = ANY(ARRAY[{parameters}])
                ORDER BY ""Id""";

            using var command = new NpgsqlCommand(query, connection, transaction);

            for (int i = 0; i < aspNetUserIds.Count; i++)
            {
                command.Parameters.AddWithValue($"@param{i}", aspNetUserIds[i]);
            }

            using var reader = await command.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                var usersId = reader.GetInt32("Id");
                var sub = reader.GetGuid("Sub");
                users.Add((usersId, sub));
            }

            return users;
        }

        private static async Task<int> DeleteUserSectionsRecords(NpgsqlConnection connection, NpgsqlTransaction transaction, int usersId)
        {
            const string query = @"DELETE FROM public.""UserSections"" WHERE ""UserId"" = @usersId";

            using var command = new NpgsqlCommand(query, connection, transaction);
            command.Parameters.AddWithValue("@usersId", usersId);

            var rowsAffected = await command.ExecuteNonQueryAsync();
            await LogAsync($"Deleted {rowsAffected} UserSections record(s) for UserId={usersId}");
            return rowsAffected;
        }

        private static async Task<int> DeleteUserDivisionsRecords(NpgsqlConnection connection, NpgsqlTransaction transaction, int usersId)
        {
            const string query = @"DELETE FROM public.""UserDivisions"" WHERE ""UserId"" = @usersId";

            using var command = new NpgsqlCommand(query, connection, transaction);
            command.Parameters.AddWithValue("@usersId", usersId);

            var rowsAffected = await command.ExecuteNonQueryAsync();
            await LogAsync($"Deleted {rowsAffected} UserDivisions record(s) for UserId={usersId}");
            return rowsAffected;
        }

        private static async Task<int> DeleteUserAgenciesRecords(NpgsqlConnection connection, NpgsqlTransaction transaction, int usersId)
        {
            const string query = @"DELETE FROM public.""UserAgencies"" WHERE ""UserId"" = @usersId";

            using var command = new NpgsqlCommand(query, connection, transaction);
            command.Parameters.AddWithValue("@usersId", usersId);

            var rowsAffected = await command.ExecuteNonQueryAsync();
            await LogAsync($"Deleted {rowsAffected} UserAgencies record(s) for UserId={usersId}");
            return rowsAffected;
        }

        private static async Task DeleteUsersRecord(NpgsqlConnection connection, NpgsqlTransaction transaction, int usersId)
        {
            const string query = @"DELETE FROM public.""Users"" WHERE ""Id"" = @usersId";

            using var command = new NpgsqlCommand(query, connection, transaction);
            command.Parameters.AddWithValue("@usersId", usersId);

            var rowsAffected = await command.ExecuteNonQueryAsync();
            await LogAsync($"Deleted {rowsAffected} Users record(s) with Id={usersId}");
        }

        private static async Task DeleteAspNetUsersRecord(NpgsqlConnection connection, NpgsqlTransaction transaction, Guid aspNetUserId)
        {
            const string query = @"DELETE FROM public.""AspNetUsers"" WHERE ""Id"" = @aspNetUserId";

            using var command = new NpgsqlCommand(query, connection, transaction);
            command.Parameters.AddWithValue("@aspNetUserId", aspNetUserId);

            var rowsAffected = await command.ExecuteNonQueryAsync();
            await LogAsync($"Deleted {rowsAffected} AspNetUsers record(s) with Id={aspNetUserId}");
        }
    }
}