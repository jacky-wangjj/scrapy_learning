# -*- coding: utf-8 -*-
import scrapy
from scrapy import Selector

from scrapy_learning.items import ScrapyLearningItem


class ExampleSpider(scrapy.Spider):
    name = "stack"
    allowed_domains = ["stackoverflow.com"]
    start_urls = ['http://stackoverflow.com/questions?pagesize=50&sort=newest']

    def parse(self, response):
        questions = Selector(response).xpath('//div[@class="summary"]/h3')
        for question in questions:
            item = ScrapyLearningItem()
            item['title'] = question.xpath('a[@class="question-hyperlink"]/text()').extract()[0]
            item['url'] = question.xpath('a[@class="question-hyperlink"]/@href').extract()[0]
            yield item