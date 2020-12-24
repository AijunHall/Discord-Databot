import discord
from discord.ext import tasks, commands
from discord.utils import get
from datetime import datetime, timedelta
import json
import mysql.connector
import numpy as np

class Config:
    @staticmethod
    def _get_tokens():
        '''
        Returns a tuple including the discord bot token, and mysql database password
        '''
        with open('.token') as f:
            token = json.load(f)

        return token['discord_token'], token['mysql_pass']

    @staticmethod
    def _get_mysql_formulas():
        '''
        Returns a json object containing mysql formulas
        '''
        with open('mysql_formulas.json') as f:
            mysql_formulas = json.load(f)

        return mysql_formulas

    @staticmethod
    def _get_connection_details():
        '''
        Returns a tuple containing mysql database connection details & discord client connection details
        '''
        with open('config.json') as f:
            config = json.load(f)

        return config['mySQL'], config['discord_bot']

    def __init__(self):
        self.bot_token, self.mysql_password = self._get_tokens()

        self.mysql_formulas = self._get_mysql_formulas()

        mysql_config, discord_config = self._get_connection_details()

        self.mysql_host = mysql_config['host']
        self.mysql_user = mysql_config['user']
        self.mysql_database = mysql_config['database']

        self.bot_command_prefix = discord_config['command_prefix']
        self.bot_activity_status = discord_config['activity_status']

class DiscordReader:

    def format_message(self, message):
        '''
        Formats, and then uploads a message/attachment to mysql database
        '''
        raw_message_string = message.clean_content
        raw_message_string = raw_message_string.replace('\n', '')
        raw_message_string = raw_message_string.replace('\\n','')
        raw_message_string = raw_message_string.lstrip()

        #Timestamp
        datetime = message.created_at

        #Embeds
        embed_there = False
        if len(message.embeds) != 0 and raw_message_string == '':
            embed_there = True
            embed = message.embeds[0]
            embed_dict = embed.to_dict()

            embed_to_message = ''
            if 'type' in embed_dict and embed_dict['type'] == 'link':
                embed_to_message += str(embed_dict['url'])
            elif 'url' in embed_dict:
                embed_to_message += str(embed_dict['url'])

            else:
                if 'title' in embed_dict:
                    title = str(embed_dict['title']).replace(' ','')
                    embed_to_message += f'{title} '

                if 'author' in embed_dict and 'name' in embed_dict['author']:
                    name = str(embed_dict['author']['name']).replace(' ','')
                    embed_to_message += f'{name} '

                if 'description' in embed_dict:
                    description = str(embed_dict['description']).replace('\r','').replace('\n','')
                    embed_to_message += f'{description} '

                if 'image' in embed_dict:
                    if ('proxy_url' in embed_dict['image']):
                        embed_to_message += f"{str(embed_dict['image']['proxy_url'])} "

                    elif 'url' in embed_dict['image']:
                        embed_to_message += f"{str(embed_dict['image']['url'])} "

        #Message
        if raw_message_string != '':
            message_str = raw_message_string

            this_message = (message.id, message.author.id, message.guild.id, message.channel.id, datetime, message_str)
            self.mycursor.execute(self.mysql_formulas['insert_messages'], this_message)

        elif embed_there:
            message_str = embed_to_message

            this_message = (message.id, message.author.id, message.guild.id, message.channel.id, datetime, message_str)
            self.mycursor.execute(self.mysql_formulas['insert_messages'], this_message)

        #Attachments
        if len(message.attachments) > 0:
            url = message.attachments[0].url
            if url.lower().endswith('.png') or url.lower().endswith('.jpg') or url.lower().endswith('.gif'):
                attachment_str = url

                this_attachment = (message.id, message.author.id, message.guild.id, message.channel.id, datetime, attachment_str)
                self.mycursor.execute(self.mysql_formulas['insert_attachments'], this_attachment)

        self.mydb.commit()

    def bot_setup(self, activity_status):
        '''
        Setup bot events
        '''
        async def initialize_database():
            '''
            Wipes database clean, and enters in new data from a fresh scan
            This can take a while
            '''
            def _fill_mysql_servers(guild):
                '''
                Fills mysql servers table
                '''
                self.mycursor.execute(f'SELECT COUNT(DISTINCT channel_id) FROM messages WHERE server_id = {guild.id}')
                channel_count = self.mycursor.fetchone()[0]

                users_count = len(guild.members)

                self.mycursor.execute(f'SELECT COUNT(*) FROM messages WHERE server_id = {guild.id}')
                message_count = self.mycursor.fetchone()[0]

                self.mycursor.execute(f'SELECT COUNT(*) FROM attachments WHERE server_id = {guild.id}')
                attachment_count = self.mycursor.fetchone()[0]

                this_server = (guild.id, channel_count, users_count, message_count, attachment_count)
                self.mycursor.execute(self.mysql_formulas['insert_servers'], this_server)

                self.mydb.commit()

            def _fill_mysql_channels(guild):
                '''
                Fills mysql channels table
                '''
                self.mycursor.execute(f"SELECT DISTINCT channel_id FROM messages WHERE server_id = {guild.id}")
                channels_list = self.mycursor.fetchall()

                for channel in channels_list:
                    channel_id = channel[0]

                    self.mycursor.execute(f"SELECT COUNT(*) FROM messages WHERE channel_id = {channel_id}")
                    channel_message_count = self.mycursor.fetchone()[0]

                    self.mycursor.execute(f"SELECT COUNT(*) FROM attachments WHERE channel_id = {channel_id}")
                    channel_attachment_count = self.mycursor.fetchone()[0]

                    this_channel = (channel_id, guild.id, channel_message_count, channel_attachment_count)
                    self.mycursor.execute(self.mysql_formulas['insert_channels'], this_channel)

                self.mydb.commit()

            def _fill_mysql_users(guild):
                '''
                Fills mysql users table
                '''
                self.mycursor.execute("SELECT DISTINCT user_id FROM users")

                all_known_unique_users = []

                for user_id in self.mycursor.fetchall():
                    all_known_unique_users.append(user_id[0])

                all_known_unique_users = tuple(all_known_unique_users)

                self.mycursor.execute(f"SELECT DISTINCT user_id FROM messages WHERE server_id = {guild.id}")
                users_with_messages_list = self.mycursor.fetchall()

                self.mycursor.execute(f"SELECT DISTINCT user_id FROM attachments WHERE server_id = {guild.id}")
                users_with_attachments_list = self.mycursor.fetchall()

                all_new_unique_users = tuple(np.unique(users_with_messages_list + users_with_attachments_list))

                for user_id in all_new_unique_users:

                    user_id = int(user_id)

                    if (user_id not in all_known_unique_users):

                        server_count = 1

                        self.mycursor.execute(f"SELECT COUNT(*) FROM messages WHERE user_id = {user_id}")
                        message_count = self.mycursor.fetchone()[0]

                        self.mycursor.execute(f"SELECT COUNT(*) FROM attachments WHERE user_id = {user_id}")
                        attachment_count = self.mycursor.fetchone()[0]

                        this_user = (user_id, server_count, message_count, attachment_count)
                        self.mycursor.execute(self.mysql_formulas['insert_users'], this_user)

                    else:

                        self.mycursor.execute(f"SELECT COUNT(DISTINCT server_id) FROM messages WHERE user_id = {user_id}")
                        server_count = self.mycursor.fetchone()[0]
                        self.mycursor.execute(f"UPDATE users SET server_count = {server_count} WHERE user_id = {user_id}")

                        self.mycursor.execute(f"SELECT COUNT(*) FROM messages WHERE user_id = {user_id}")
                        message_count = self.mycursor.fetchone()[0]
                        self.mycursor.execute(f"UPDATE users SET message_count = {message_count} WHERE user_id = {user_id}")

                        self.mycursor.execute(f"SELECT COUNT(*) FROM attachments WHERE user_id = {user_id}")
                        attachment_count = self.mycursor.fetchone()[0]
                        self.mycursor.execute(f"UPDATE users SET attachment_count = {attachment_count} WHERE user_id = {user_id}")

                self.mydb.commit()

            def _empty_mysql_tables(guild_id):
                '''
                Wipe entire database
                '''
                self.mycursor.execute(f"{self.mysql_formulas['delete_servers']}{guild_id}")
                self.mycursor.execute(f"{self.mysql_formulas['delete_messages']}{guild_id}")
                self.mycursor.execute(f"{self.mysql_formulas['delete_attachments']}{guild_id}")
                self.mycursor.execute(f"{self.mysql_formulas['delete_channels']}{guild_id}")

            all_visible_guilds = self.client.guilds
            for guild in all_visible_guilds:

                _empty_mysql_tables(guild.id)

                print(f'Reading [{guild.name}] history...\n')

                #First use enterServerHistory() to fill out messages and attachments tables
                all_channel_history = []
                for channel in guild.text_channels:
                    try:
                        print(f'Reading #{channel} message history...')
                        self.channels_scanned.append(channel.id)
                        all_channel_history += await channel.history(limit=None).flatten()
                        print(f'Finished reading #{channel} message history\n')
                    except discord.Forbidden:
                        print(f'\nERROR: {channel.name} could not be read\n')

                for message in tuple(all_channel_history):
                    self.format_message(message)

                _fill_mysql_servers(guild)
                _fill_mysql_channels(guild)
                _fill_mysql_users(guild)

                print('Finished reading server history\n')

            print('Finished database entry\n')

        @self.client.event
        async def on_ready():
            print('Bot is running')

            await self.client.change_presence(activity=discord.Game(activity_status))

            if self.initialized == False:
                self.initialized = True
                await initialize_database()
                
        @self.client.event
        async def on_message(message):
            if message.author.id == 188701887451627520 and message.content == '>>shutdown':
                print('Bot shutdown')
                await self.client.logout()

            elif message.guild != None and message.channel.id in self.channels_scanned:
                self.format_message(message)

        @self.client.event
        async def on_message_edit(_, new_message):
            if new_message.guild != None:
                self.mycursor.execute(f'DELETE FROM messages WHERE message_id = {new_message.id}')
                self.mycursor.execute(f'DELETE FROM attachments WHERE message_id = {new_message.id}')
                self.format_message(new_message)

        @self.client.event
        async def on_raw_message_delete(deleted_message):
            if deleted_message.guild_id != None:
                self.mycursor.execute(f'DELETE FROM messages WHERE message_id = {deleted_message.message_id}')
                self.mycursor.execute(f'DELETE FROM attachments WHERE message_id = {deleted_message.message_id}')
                self.mydb.commit()

        @self.client.event
        async def on_raw_bulk_message_delete(deleted_bulk_messages):
            if deleted_bulk_messages.guild_id != None:
                for message_id in deleted_bulk_messages.message_ids:
                    self.mycursor.execute(f'DELETE FROM messages WHERE message_id = {message_id}')
                    self.mycursor.execute(f'DELETE FROM attachments WHERE message_id = {message_id}')
                    self.mydb.commit()

    def __init__(self):
        config = Config()

        self.initialized = False
        self.channels_scanned = []

        self.mydb = mysql.connector.connect(

            host = config.mysql_host,
            user = config.mysql_user,
            passwd = config.mysql_password,
            database = config.mysql_database
        )

        self.mycursor = self.mydb.cursor(buffered=True)
        self.mysql_formulas = config.mysql_formulas

        self.client = commands.Bot(command_prefix=config.bot_command_prefix)
        self.client.remove_command('help')

        self.bot_setup(config.bot_activity_status)
        self.client.run(config.bot_token)

tester = DiscordReader()
