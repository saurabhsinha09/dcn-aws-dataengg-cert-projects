aws ecr get-login-password \
        --region ap-south-1 | docker login \
        --username AWS \
        --password-stdin <aws-account-id>.dkr.ecr.ap-south-1.amazonaws.com

docker build -t mobile-monitor-app .

docker run -p 8501:5002 -v ${HOME}/.aws:/root/.aws mobile-monitor-app

docker tag mobile-monitor-app:latest <aws-account-id>.dkr.ecr.ap-south-1.amazonaws.com/telecom-provider-app:mobile-monitor-app

docker push <aws-account-id>.dkr.ecr.ap-south-1.amazonaws.com/telecom-provider-app:mobile-monitor-app