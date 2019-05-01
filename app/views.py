# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import csv
import pandas as pd
from django.shortcuts import render
from django.http import HttpResponse
from django.http import Http404
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from .join_transform import transform_join, transform_sort

class CollectSchema(APIView):
    def get(self, request, format=None):
        return Response({
            'hello': 'world'
        })

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
            
class CsvTransform(APIView):
    def post(self, request, format=None):
        try:
            column_name_a = request.POST.get('column_name_a', '')
            column_name_b = request.POST.get('column_name_b', '')
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
            if downloadable:
                response = HttpResponse(content_type='text/csv')
                response['Content-Disposition'] = 'attachment; filename=csvtransform.csv'
                df_r.to_csv(path_or_buf=response,index=False)
                return response
            else:
                return Response({
                    'df_r': df_r.to_csv(index=False)
                })
        except:
            return Response({'message': 'unknown'}, status=status.HTTP_400_BAD_REQUEST)
