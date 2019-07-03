FROM python:3-alpine

# Configure environment
ENV LANG=C.UTF-8 \
    LC_ALL=C.UTF-8
	
ENV	http_proxy="http://10.0.75.1:3128" \
	https_proxy="http://10.0.75.1:3128" \
	HTTP_PROXY="http://10.0.75.1:3128" \
	HTTPS_PROXY="http://10.0.75.1:3128"

# for pip installs speedups
RUN apk add --no-cache gcc musl-dev linux-headers

# set up python app
WORKDIR /usr/src/app
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

CMD ["python3", "./main.py"]