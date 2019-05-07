# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import sys
import csv
import pandas as pd
import mysql.connector as mysql
from django.shortcuts import render
from django.http import HttpResponse
from django.http import Http404
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from .join_transform import transform_join, transform_sort

class Database2Schema(APIView):
    def post(self, request, format=None):
        try:
            db = mysql.connect(
                host = request.POST.get('mysql_host', 'localhost'),
                user = request.POST.get('mysql_user', ''),
                passwd = request.POST.get('mysql_passwd', ''))
            c = db.cursor()
            
            def databases():
                c.execute('show databases')
                database_names = c.fetchall()
                database_names = list(map(lambda e: e[0], database_names))
                return database_names

            def tables(database_name):
                c.execute('use {}'.format(database_name))
                c.execute('show tables')
                table_names = c.fetchall()
                table_names = list(map(lambda e: e[0], table_names))
                return table_names

            def fields(database_name, table_name):
                c.execute('use {}'.format(database_name))
                c.execute('describe {}'.format(table_name))
                field_names = c.fetchall()
                field_names = list(map(lambda e: e[0], field_names))
                return field_names

            schema = []
            for d in databases():
                for t in tables(d):
                    for f in fields(d, t):
                       schema.append('{0}.{1}.{2}'.format(d, t, f)) 
            c.close()
            db.close()
            return Response(schema)
        except:
            return Response({'message': 'unknown'}, status=status.HTTP_400_BAD_REQUEST)

class Database2Head(APIView):
    def post(self, request, format=None):
        try:
            def sql2pd(host, user, passwd, database_name, table_name):
                db = mysql.connect(
                    host = host,
                    user = user,
                    passwd = passwd,
                    database = database_name)
                df = pd.read_sql('select * from {}'.format(table_name), con=db)
                db.close()
                return df
     
            mysql_host = request.POST.get('mysql_host', 'localhost')
            mysql_user = request.POST.get('mysql_user', '')
            mysql_passwd = request.POST.get('mysql_passwd', '')
            left_column = request.POST.get('left_column', '')
            right_column = request.POST.get('right_column', '')
            join_type = request.POST.get('join_type', '')
            left_column = left_column.strip().split('.')
            right_column = right_column.strip().split('.')
            database_name_a = left_column[0]
            database_name_b = right_column[0]
            table_name_a = left_column[1]
            table_name_b = right_column[1]
            column_name_a = left_column[2]
            column_name_b = right_column[2]
            df_a = sql2pd(
                host = mysql_host,
                user = mysql_user,
                passwd = mysql_passwd,
                database_name = database_name_a,
                table_name = table_name_a)
            df_b = sql2pd(
                host = mysql_host,
                user = mysql_user,
                passwd = mysql_passwd,
                database_name = database_name_b,
                table_name = table_name_b)
            df_r = transform_join(df_a, df_b, column_name_a, column_name_b, join_type)
            return Response(list(df_r))
        except:
            return Response({'message': 'unknown'}, status=status.HTTP_400_BAD_REQUEST)

class DatabaseTransform(APIView):
    def post(self, request, format=None):
        try:
            def sql2pd(host, user, passwd, database_name, table_name):
                db = mysql.connect(
                    host = host,
                    user = user,
                    passwd = passwd,
                    database = database_name)
                df = pd.read_sql('select * from {}'.format(table_name), con=db)
                db.close()
                return df
     
            mysql_host = request.POST.get('mysql_host', 'localhost')
            mysql_user = request.POST.get('mysql_user', '')
            mysql_passwd = request.POST.get('mysql_passwd', '')
            left_column = request.POST.get('left_column', '')
            right_column = request.POST.get('right_column', '')
            join_type = request.POST.get('join_type', '')
            left_column = left_column.strip().split('.')
            right_column = right_column.strip().split('.')
            database_name_a = left_column[0]
            database_name_b = right_column[0]
            table_name_a = left_column[1]
            table_name_b = right_column[1]
            column_name_a = left_column[2]
            column_name_b = right_column[2]       
            sort_column = request.POST.get('sort_column', '')
            sort_type = bool(request.POST.get('sort_type', False))
            downloadable = bool(request.POST.get('downloadable', False))
            df_a = sql2pd(
                host = mysql_host,
                user = mysql_user,
                passwd = mysql_passwd,
                database_name = database_name_a,
                table_name = table_name_a)
            df_b = sql2pd(
                host = mysql_host,
                user = mysql_user,
                passwd = mysql_passwd,
                database_name = database_name_b,
                table_name = table_name_b)
            df_r = transform_join(df_a, df_b, column_name_a, column_name_b, join_type)
            df_r = transform_sort(df_r, sort_column, sort_type)
            df_r = df_r.replace('\r', '', regex=True)
            df_r = df_r.replace('\n', '', regex=True)
            if downloadable:
                response = HttpResponse(content_type='text/csv')
                response['Content-Disposition'] = 'attachment; filename=databasetransform.csv'
                df_r.to_csv(path_or_buf=response,index=False)
                return response
            else:
                return Response({
                    'df_r': df_r.to_csv(index=False).strip('\n')
                })
        except:
            return Response({'message': 'unknown'}, status=status.HTTP_400_BAD_REQUEST)

class Csv2Head(APIView):
    def post(self, request, format=None):
        try:
            files = request.FILES.getlist('csv_files')
            n_files = len(files)
            file_names = list(map(lambda f: f.file.name, files))
            if n_files != 2:
                return Response({'message': 'n_files != 2'}, status=status.HTTP_400_BAD_REQUEST)
            df_a = pd.read_csv(file_names[0])
            df_b = pd.read_csv(file_names[1])
            return Response({
                'df_a': ','.join(list(df_a)),
                'df_b': ','.join(list(df_b))
            })
        except:
            return Response({'message': 'unknown'}, status=status.HTTP_400_BAD_REQUEST)

class Csv2JoinHead(APIView):
    def post(self, request, format=None):
        try:
            column_name_a = request.POST.get('left_column', '')
            column_name_b = request.POST.get('right_column', '')
            join_type = request.POST.get('join_type', '')
            files = request.FILES.getlist('csv_files')
            n_files = len(files)
            file_names = list(map(lambda f: f.file.name, files))
            if n_files != 2:
                return Response({'message': 'n_files != 2'}, status=status.HTTP_400_BAD_REQUEST)
            df_a = pd.read_csv(file_names[0])
            df_b = pd.read_csv(file_names[1])
            df_r = transform_join(df_a, df_b, column_name_a, column_name_b, join_type)
            return Response(list(df_r))
        except:
            return Response({'message': 'unknown'}, status=status.HTTP_400_BAD_REQUEST)
            
class CsvTransform(APIView):
    def post(self, request, format=None):
        try:
            column_name_a = request.POST.get('left_column', '')
            column_name_b = request.POST.get('right_column', '')
            join_type = request.POST.get('join_type', '')
            sort_column = request.POST.get('sort_column', '')
            sort_type = bool(request.POST.get('sort_type', False))
            downloadable = bool(request.POST.get('downloadable', False))
            files = request.FILES.getlist('csv_files')
            n_files = len(files)
            file_names = list(map(lambda f: f.file.name, files))
            if n_files != 2:
                return Response({'message': 'n_files != 2'}, status=status.HTTP_400_BAD_REQUEST)
            df_a = pd.read_csv(file_names[0])
            df_b = pd.read_csv(file_names[1])
            df_r = transform_join(df_a, df_b, column_name_a, column_name_b, join_type)
            df_r = transform_sort(df_r, sort_column, sort_type)
            df_r = df_r.replace('\r', '', regex=True)
            df_r = df_r.replace('\n', '', regex=True)
            if downloadable:
                response = HttpResponse(content_type='text/csv')
                response['Content-Disposition'] = 'attachment; filename=csvtransform.csv'
                df_r.to_csv(path_or_buf=response,index=False)
                return response
            else:
                return Response({
                    'df_r': df_r.to_csv(index=False).strip('\n')
                })
        except:
            return Response({'message': 'unknown'}, status=status.HTTP_400_BAD_REQUEST)
