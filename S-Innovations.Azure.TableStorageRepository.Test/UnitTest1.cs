using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage;
using System.Linq;
using Microsoft.WindowsAzure.Storage.Auth;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.IO;
using SInnovations.Azure.TableStorageRepository.TableRepositories;
using SInnovations.Azure.TableStorageRepository.DataInitializers;

namespace SInnovations.Azure.TableStorageRepository.Test
{

#if DEBUG

    public class Family : TableEntity
    {
        public string FamilyName { get; set; }

        public virtual ICollection<Person> People { get; set; }
    }
    public class Person : TableEntity
    {
        public string FamilyName { get; set; }
        public string FirstName { get;set; }
        public virtual ICollection<Pet> Pets { get; set; }
        public virtual ICollection<IdentityUserLogin<String>> Logins { get; set; }
    }
    public class UserLogin :IdentityUserLogin<Guid>
    {

    }
    public class IdentityUserLogin<TKey> : TableEntity
    {
        // Methods
        public IdentityUserLogin() { }

        // Properties
        public virtual string LoginProvider { get; set; }
        public virtual string ProviderKey { get; set; }
        public virtual TKey UserId { get; set; }


    }
    public class Pet : TableEntity
    {

        public string FamilyName { get; set; }
        public string PetName { get; set; }


    }

    public class MyTableStorageContext : TableStorageContext
    {   
        static MyTableStorageContext()
        {
            Table.SetInitializer(new CreateTablesIfNotExists<MyTableStorageContext>());
        }
        public string test {get;set;}
        public MyTableStorageContext() : base(CloudStorageAccount.Parse(File.ReadAllText("C:\\dev\\storagekey.txt")), true)
        {
            test = "a";
            this.InsertionMode = InsertionMode.AddOrMerge;
        }
        protected override void OnModelCreating(TableStorageModelBuilder modelbuilder, params object[] modelBuilderParams)
        {
            modelbuilder.Entity<Person>().HasKeys((c) => c.FamilyName, c => c.FirstName)
                .WithIndex(p=>p.FirstName)
                .WithCollectionOf<Pet>(u=>u.Pets, (s,f) => (from ent in s where ent.FamilyName ==f.FamilyName select ent))
                .ToTable("People");
            modelbuilder.Entity<Pet>()
                .HasKeys((c) => c.FamilyName, c => c.PetName)
                .ToTable("Pets");
            modelbuilder.Entity<UserLogin>()
            .HasKeys((ul) => ul.UserId,
                        (ul) => new { ul.LoginProvider, ul.ProviderKey })
            .WithIndex(ul => new { ul.LoginProvider, ul.ProviderKey })
            .ToTable("AspNetUserLogins");

            modelbuilder.Entity<Family>()
                .HasKeys(f => f.FamilyName, f => "")
                .WithCollectionOf<Person>(f => f.People, (source, family) => (from ent in source where ent.PartitionKey == family.FamilyName select ent))
                .ToTable("AspNetFamility");
        }

        public ITableRepository<Person> People { get; set; }
        public ITableRepository<Pet> Pets { get; set; }
        public ITableRepository<UserLogin> Logins { get; set; }

        public ITableRepository<Family> Families { get; set; }


    }
    [TestClass]
    public class UnitTest1
    {
                
        [TestMethod]
        public async Task WithCollectionTest()
        {
            var context = new MyTableStorageContext();

            var familiy = new Family(){ FamilyName ="Sorensen"};
            context.Families.Add(familiy);
            context.Families.Add(new Family { FamilyName = "Suikkanen" });
            context.People.Add(new Person { FamilyName = "Sorensen",FirstName="Poul" });
            context.People.Add(new Person { FamilyName = "Sorensen", FirstName = "Torben" });
            context.People.Add(new Person { FamilyName = "Suikkanen", FirstName = "Naja" });
            context.People.Add(new Person { FamilyName = "Suikkanen", FirstName = "Aimo" });
            await context.SaveChangesAsync();
            
            var sorensen = await context.Families.FindByKeysAsync("Sorensen", "");
            var suikkanen = await context.Families.FindByKeysAsync("Suikkanen", "");
            var peopleQuery = from person in sorensen.People                            
                              select person;

            var peopleInFam = peopleQuery.ToArray();
            
            var suikannanesServerFilter = (from ent in context.People
                                          where ent.PartitionKey == "Suikkanen"
                                          select ent).ToArray();

            var suikkanens = (from ent in suikkanen.People.AsQueryable()
                             where ent.FirstName.CompareTo("Na") > 0
                             select ent).ToArray();

            var a = await context.People.FindByIndexAsync("Poul");
        }
 
        [TestMethod]
        public async Task InsertionTest()
        {
            System.Net.ServicePointManager.DefaultConnectionLimit = 1000; 
            var context = new MyTableStorageContext();
            //  var table = context.GetTable<UserLogin>();
            //  await table.DeleteIfExistsAsync();
            //  await table.CreateIfNotExistsAsync();
            {
                int i = 5;
                for (int n = 0; n < i; n++)
                {
                    context.Logins.Add(new UserLogin() { UserId = Guid.NewGuid(), LoginProvider = RandomLoginProvider(), ProviderKey = Guid.NewGuid().ToString() });

                }

                await context.SaveChangesAsync();
            }
            {
                int i = 500;
                for (int n = 0; n < i; n++)
                {
                    context.Logins.Add(new UserLogin() { UserId = Guid.NewGuid(), LoginProvider = RandomLoginProvider(), ProviderKey = Guid.NewGuid().ToString() });

                }

                await context.SaveChangesAsync();
            }




        }

        private string RandomLoginProvider()
         {
             var n= new Random().Next(5);
             switch (n)
             {
                 case 0:
                     return "Google";
                 case 1:
                     return "Facebook";
                 case 2:
                     return "Twitter";
                 case 3:
                     return "MicrosoftLive";
                 case 4:
                     return "WAAD";
                 default:
                     return "UNKNOWN";
                                  }
         }

        [TestMethod]
        public void TestMethod1()
        {
            // This test only work local when connectionstring is updated.

            var context = new MyTableStorageContext();
            context.People.Add(new Person() { FamilyName="Sorensen", FirstName="Poul"});
            context.People.Add(new Person() { FamilyName = "Sorensen", FirstName = "Karsten" });
            context.People.Add(new Person() { FamilyName = "Sorensen", FirstName = "Annette" });
            context.People.Add(new Person() { FamilyName = "Sorensen", FirstName = "Torben" });
            context.People.Add(new Person() { FamilyName = "Sorensen", FirstName = "Peter" });
            context.Pets.Add(new Pet { PetName = "Chili", FamilyName="Sorensen" });
            context.Pets.Add(new Pet { PetName = "Bandit", FamilyName = "Sorensen" });
            context.Pets.Add(new Pet { PetName = "HendeDenHvide", FamilyName = "Sorensen" });
            context.SaveChangesAsync().Wait();

            var iqueryable_querys = from ent in context.Pets
                                    where ent.PartitionKey == "Sorensen"
                                    select ent;

            var arr0 = iqueryable_querys.ToArray();
            // Above only works when model type has base of TableEntity.
            //Else fluent querys are needed.
            var arr1 = context.People.FluentQuery(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "Sorensen")).ToArray();

           var a = context.People.FindByIndexAsync("Karsten").Result;
           Console.WriteLine(a);
        }

        [TestMethod]
        public void TestMethod2()
        {
            string test = "";
            var c = new Pet();
            EntityTypeConfiguration<Pet>.ConvertToStringKey(t => new { t.FamilyName, t.RowKey }, out test,new string[]{});


        }
    }
#endif
}
