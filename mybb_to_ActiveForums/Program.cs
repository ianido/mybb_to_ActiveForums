using DataAccess;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;

namespace mybb_to_ActiveForums
{
    class Program
    {
        private static int ModuleID = 453;
        private static string DNNPortalRoot = @"\\miasawst02\d$\WebSites\portal.dev.mia.amadeus.net\Portals\0\activeforums_Attach\";
        private static string myBBRoot = @"\\MIASRVRTSIWS1\RTSWorkbench_Share\wb\TOPSForum\uploads\";

        public static DateTime ConvertFromUnixTimestamp(double timestamp)
        {
            DateTime origin = new DateTime(1970, 1, 1, 0, 0, 0, 0);
            return origin.AddSeconds(timestamp);
        }

        public static double ConvertFromTimestamptoUnix(DateTime date)
        {
            TimeSpan origin = date - new DateTime(1970, 1, 1, 0, 0, 0, 0);
            return origin.TotalSeconds;
        }

        public static DateTime Min(DateTime x, DateTime y)
        {
            return (x.ToUniversalTime() < y.ToUniversalTime()) ? x : y;
        }

        public static am_User FindDNNUser(string username)
        {
            afDB af = new afDB();
            var user = af.FirstOrDefault<am_User>("where Username like @0", "%" + username);
            if (user == null)
                user = af.FirstOrDefault<am_User>("where Email like @0", username + "@%");
            if (user == null)
            {
                user = new am_User();
                user.UserID = -1;
                user.Username = username;
            }
            return user;
        }

        public static void DeleteInformation()
        {
            afDB af = new afDB();
            af.Execute("delete FROM [dbo].[am_activeforums_ForumTopics]");
            af.Execute("delete FROM [dbo].[am_activeforums_Content]");
            af.Execute("delete FROM [dbo].[am_activeforums_Topics]");
            af.Execute("delete FROM [dbo].[am_activeforums_Replies]");
            af.Execute("delete FROM [dbo].[am_activeforums_Attachments]");
            af.Execute("delete FROM [dbo].[am_activeforums_Forums]");
        }

        public static void MigrateForums(){
             mbbDB mm = new mbbDB();
            List<mybb_forum> mforumns = mm.Fetch<mybb_forum>("where 1=1");
            afDB af = new afDB();


            // ActiveForums        myBB
            // Topics        --> Threads
            // Replies       --> Posts
            

            // En myBB cuando se crea un thread, tambien se crea un post
            // En myBB cuando se crea un post es como un reply en AF

            // En AF cuando se crea un Topic se crea un activeforum_Topic y un activeforum_Content
            // En AF cuando se crea un Reply se crea un activeforum_Replies y un activeforum_Content

            foreach (var forum in mforumns)
            {
                if (forum.posts == 0) continue;

                var firstpost = mm.FirstOrDefault<mybb_post>("where fid=@0 order by dateline asc", forum.fid);
                var lastpost = mm.FirstOrDefault<mybb_post>("where fid=@0 order by dateline desc", forum.fid);
                var lastpostuser = af.FirstOrDefault<am_User>("where Username like @0", "%" + forum.lastposter);

                var firstThread = mm.FirstOrDefault<mybb_thread>("where fid=@0 order by dateline asc", forum.fid);
                var lastThread = mm.FirstOrDefault<mybb_thread>("where fid=@0 order by dateline desc", forum.fid);
                var lastThreaduser = af.FirstOrDefault<am_User>("where Username like @0", "%" + lastThread.username);

                am_activeforums_Forum newForum = new am_activeforums_Forum();

                newForum.Active = forum.active == 1;
                if (firstpost != null) 
                    newForum.DateCreated = ConvertFromUnixTimestamp(firstpost.dateline); 
                else 
                    newForum.DateCreated = DateTime.UtcNow;

                if (lastpost != null)
                {
                    newForum.DateUpdated = ConvertFromUnixTimestamp(lastpost.dateline).ToUniversalTime();
                    newForum.LastPostDate = ConvertFromUnixTimestamp(lastThread.dateline).ToUniversalTime();
                    //newForum.LastPostId = Convert.ToInt32(lastThread.tid);
                    newForum.LastPostSubject = lastThread.subject;
                    newForum.LastPostAuthorId = lastThreaduser.UserID;
                    newForum.LastPostAuthorName = lastThreaduser.DisplayName;
                }
                else
                    newForum.DateUpdated = DateTime.UtcNow;

                newForum.ForumGroupId = 1;
                newForum.ParentForumId = 0;
                newForum.ForumName = forum.name;
                newForum.ForumSecurityKey = "G:1";
                newForum.ForumSettingsKey = "G:1";
                newForum.PortalId = 0;
                newForum.ModuleId = ModuleID;
                newForum.SortOrder = forum.disporder;
                newForum.ForumDesc = forum.description;

                newForum.LastTopicId = 0;
                newForum.LastReplyId = 0;
                newForum.Hidden = false;
                newForum.TotalTopics = 0;
                newForum.TotalReplies = 0;
                

                newForum.PermissionsId = 1;
                newForum.PrefixURL = "";
                newForum.SocialGroupId = 0;
                newForum.HasProperties = false;


                
                af.Insert(newForum);
            } 
        }

        public static void MigrateThreadsAndPosts()
        {
            Dictionary<uint,int> mapReplies = new Dictionary<uint,int>();
            mbbDB mm = new mbbDB();
            afDB af = new afDB();
            List<mybb_thread> mthreads = mm.Fetch<mybb_thread>("where 1=1 order by tid");
            
            // ActiveForums        myBB
            // Topics        --> Threads
            // Replies       --> Posts


            // En myBB cuando se crea un thread, tambien se crea un post
            // En myBB cuando se crea un post es como un reply en AF

            // En AF cuando se crea un Topic se crea un activeforum_Topic y un activeforum_Content
            // En AF cuando se crea un Reply se crea un activeforum_Replies y un activeforum_Content

            foreach (var thread in mthreads)
            {                
                var posts = mm.Fetch<mybb_post>("where tid=@0 order by dateline asc", thread.tid);

                if (posts.Count == 0) {Console.WriteLine("No posts for threadid:" + thread.tid + ", Subject='" + thread.subject + "'"); continue;}
                var threadpost = posts[0];

                var threadforum = mm.FirstOrDefault<mybb_forum>("where fid=@0", thread.fid);
                var threaduser = FindDNNUser(thread.username);
                var afforum = af.FirstOrDefault<am_activeforums_Forum>("where ForumName = @0", threadforum.name);
                int postnum = 0;

                am_activeforums_Topic newTopic = new am_activeforums_Topic();
                am_activeforums_ForumTopic newForumTopic = new am_activeforums_ForumTopic();

                foreach (var post in posts){
                    var replyto = mm.FirstOrDefault<mybb_post>("where pid=@0", post.replyto);
                    var postuser = FindDNNUser(post.username);

                    am_activeforums_Content newContent = new am_activeforums_Content();

                    newContent.AuthorId = postuser.UserID;
                    newContent.AuthorName = postuser.DisplayName;
                    newContent.DateCreated = ConvertFromUnixTimestamp(post.dateline);
                    newContent.DateUpdated = ConvertFromUnixTimestamp(post.dateline);

                    newContent.Subject = post.subject;
                    newContent.Summary = post.subject;
                    newContent.Body = post.message;
                    newContent.AuthorId = postuser.UserID;
                    newContent.AuthorName = postuser.DisplayName;
                    newContent.IPAddress = post.ipaddress;
                    newContent.IsDeleted = (post.visible != 1);
                
                    af.Insert(newContent);

                    if (postnum == 0)
                    {
                        //af.Execute("delete from am_activeforums_Topic where ");

                        newTopic.ContentId = newContent.ContentId;
                        newTopic.IsApproved = thread.visible == 1;
                        newTopic.TopicType = 0;
                        newTopic.Priority = 0;
                        newTopic.URL = "";
                        newTopic.TopicData = "";
                        newTopic.IsLocked = (thread.closed == "1");
                        newTopic.IsDeleted = (thread.visible != 1);
                        newTopic.IsArchived = false;
                        newTopic.IsApproved = true;
                        newTopic.IsAnnounce = false;
                        newTopic.IsRejected = false;
                        newTopic.TopicIcon = "";
                        newTopic.ViewCount = thread.views;
                        newTopic.ReplyCount = thread.replies;
                        newTopic.AnnounceStart = null;
                        newTopic.AnnounceEnd = null;

                        af.Insert(newTopic);
                        
                        newForumTopic.ForumId = afforum.ForumId;
                        newForumTopic.TopicId = newTopic.TopicId;
                        newForumTopic.LastTopicDate = Convert.ToInt32(ConvertFromTimestamptoUnix(newContent.DateCreated));
                        newForumTopic.LastReplyDate = Convert.ToInt32(ConvertFromTimestamptoUnix(newContent.DateCreated));
                        newForumTopic.LastReplyId = null;

                        af.Insert(newForumTopic);

                        afforum.TotalTopics += 1;
                        afforum.LastTopicId = newTopic.TopicId;
                    }
                    else
                    {
                        am_activeforums_Reply newReply = new am_activeforums_Reply();
                        newReply.ContentId = newContent.ContentId;
                        newReply.IsApproved = (post.visible == 1);
                        newReply.IsRejected = (post.visible != 1);
                        newReply.IsDeleted = (post.visible != 1);
                        newReply.StatusId = 0;
                        newReply.TopicId = newTopic.TopicId;
                        if ((mapReplies.ContainsKey(post.replyto)) && (replyto != null))
                            newReply.ReplyToId = mapReplies[post.replyto];

                        af.Insert(newReply);
                        mapReplies.Add(post.pid, newReply.ReplyId);

                        newForumTopic.LastReplyId = newReply.ReplyId;
                        newForumTopic.LastReplyDate = Convert.ToInt32(ConvertFromTimestamptoUnix(newContent.DateCreated));
                        af.Update(newForumTopic );

                        afforum.TotalReplies += 1;
                        afforum.LastReplyId = newReply.ReplyId;
                    }

                    if (thread.attachmentcount > 0)
                    {
                        var attachments = mm.Fetch<mybb_attachment>("where pid=@0", post.pid);
                        foreach (var attach in attachments)
                        {
                            if (File.Exists(Path.Combine(myBBRoot, attach.attachname)))
                            {
                                if (!File.Exists(Path.Combine(DNNPortalRoot, "__" + newContent.ContentId + "__0__" + attach.filename))) 
                                    File.Copy(Path.Combine(myBBRoot, attach.attachname), Path.Combine(DNNPortalRoot, "__" + newContent.ContentId + "__0__" + attach.filename));
                            }

                            var attuser = mm.FirstOrDefault<mybb_user>("where uid=@0", attach.uid);
                            var afuser = FindDNNUser(attuser.username);

                            am_activeforums_Attachment newAttach = new am_activeforums_Attachment();
                            newAttach.ContentId = newContent.ContentId;
                            newAttach.ContentType = attach.filetype;
                            newAttach.AllowDownload = true;
                            newAttach.DownloadCount = Convert.ToInt32(attach.downloads);
                            newAttach.DisplayInline = false;
                            newAttach.FileSize = attach.filesize;
                            newAttach.UserID = afuser.UserID;
                            newAttach.DateUpdated = newAttach.DateAdded = attach.dateuploaded == 0 ? (DateTime?)null : ConvertFromUnixTimestamp(attach.dateuploaded);
                            newAttach.ParentAttachId = 0;
                            newAttach.FileData = new byte[0];
                            newAttach.Filename = "__" + newContent.ContentId + "__0__" + attach.filename;

                            af.Insert(newAttach);
                        }
                    }

                    afforum.LastPostAuthorId = postuser.UserID;
                    afforum.LastPostAuthorName = postuser.DisplayName;
                    afforum.LastPostSubject = newContent.Subject;
                    afforum.LastPostDate = newContent.DateCreated;

                    af.Update(afforum);

                    postnum++;
                }
            }            

        }
        
        public static void MigrateAttachments()
        {
            afDB af = new afDB();
            af.Execute("update am_activeforums_Attachments set FileData = null");
            var attachments = af.Fetch<am_activeforums_Attachment>("where 1=1");
            foreach (var attach in attachments)
            {
                var file = af.FirstOrDefault<am_File>("where FileName = @0", attach.Filename);
                if (file != null)
                {
                    attach.FileID = file.FileId;
                    attach.FileData = new byte[0];
                    af.Update(attach);
                }
            }
            af.Execute("update am_activeforums_Attachments set FileData = null");
        }

        static void Main(string[] args)
        {
            if (!string.IsNullOrEmpty(ConfigurationManager.AppSettings["ModuleID"])) ModuleID = int.Parse(ConfigurationManager.AppSettings["ModuleID"]);
            if (!string.IsNullOrEmpty(ConfigurationManager.AppSettings["DNNPortalRoot"])) DNNPortalRoot = ConfigurationManager.AppSettings["DNNPortalRoot"];
            if (!string.IsNullOrEmpty(ConfigurationManager.AppSettings["myBBRoot"])) myBBRoot = ConfigurationManager.AppSettings["myBBRoot"];
            
            Console.WriteLine("Start Forum Migration");
            Console.WriteLine("First - Delete all existing forums information");
            Console.ReadLine();

            DeleteInformation();

            Console.WriteLine("Press enter to start Forums migration");
            Console.ReadLine();

            MigrateForums();

            Console.WriteLine("Migrated Forums");
            Console.WriteLine("Start migrating posts...");

            MigrateThreadsAndPosts();

            Console.WriteLine("Migrated Posts and attachments");
            Console.WriteLine("Proceed to synchronize files in DNN before the next step. (Admin --> File Manager --> Synchronize Folder & Subfolders");
            Console.ReadLine();
            
            // You have to synchronize your DNN files before run the next

            MigrateAttachments();

            Console.WriteLine("Migrated Attachments");
            Console.ReadLine();
        }



        
    }
}
