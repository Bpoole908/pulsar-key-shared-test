# !/bin/bash

alias devup="docker-compose -f docker-compose-dev.yml up -d"
alias devstop="docker-compose -f docker-compose-dev.yml stop"
alias devdown="docker-compose -f docker-compose-dev.yml down -v"
alias up="docker-compose up -d"
alias stop="docker-compose stop"
alias down="docker-compose down -v"