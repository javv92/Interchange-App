pipeline {
    agent any

    options {
       disableConcurrentBuilds()
    }

    environment {
      ENVIRONMENT = getEnvironment()
      APP_STACK_NAME = "app-interchange-stack-${ENVIRONMENT}"
      DEVOPS_STACK_NAME = 'app-interchange-devops-stack'
    }

    stages {
        stage('Preparing Configurations'){
            steps {
                script{
                    switch (ENVIRONMENT) {
                        case "dev":
                            ARTIFACT_BUCKET = "itl-0009-devops-all-s3-main-01"
                            break
                        case "prd":
                            ARTIFACT_BUCKET = "intelica-devops"
                            break
                        default:
                            ARTIFACT_BUCKET = ""
                    }

                    SHORT_COMMIT = getShortCommitId()
                    LAMBDA_APP_ZIP_NAME = "Lambda-app-${SHORT_COMMIT}.zip"
                    APP_ZIP_NAME = "App-${SHORT_COMMIT}.zip"

                    sh (returnStdout: true, script: "aws s3 cp s3://${ARTIFACT_BUCKET}/app-interchange/config/${ENVIRONMENT}/.env ${workspace}/.env", label: "Download config file")                
                }
            }
        }

        /*stage('Infrastructure Provisioning'){
            steps {
                script {
                    sh (returnStdout: true, script: "aws s3 cp infrastructure/cfn-main-template.yaml s3://${ARTIFACT_BUCKET}/app-interchange/cloudformation-templates/${ENVIRONMENT}/cfn-main-template.yaml", label: "Uploading from s3")

                    dir('infrastructure'){

                          appStackStatus = getStackStatus(APP_STACK_NAME)

                          if(appStackStatus == "ROLLBACK_COMPLETE"){
                              sh (returnStdout: false, script: "aws cloudformation delete-stack --stack-name ${APP_STACK_NAME}", label: "Delete Stack ..." )

                              sh (returnStdout: false, script: "aws cloudformation wait stack-delete-complete --stack-name ${APP_STACK_NAME}", label: "Waiting for Stack Deleted ..." )
                          }

                          def deployScriptString = "aws cloudformation deploy --template-file ./cfn-main-template.yaml --stack-name ${APP_STACK_NAME} --capabilities CAPABILITY_NAMED_IAM \
                              --parameter-overrides ${readYamlParameters()} \
                              --tags Environment=${ENVIRONMENT} Project=AppInterchange"

                          sh (returnStdout: false, script: deployScriptString, label: "Deploying App stack ..." )

                          appStackStatus = getStackStatus(APP_STACK_NAME)

                          getStackStatus = sh (returnStdout: true, script: "aws cloudformation describe-stacks --stack-name ${APP_STACK_NAME}", label: "Describe App Stack").trim()
                          jsonResult = readJSON text: getStackStatus

                          switch (ENVIRONMENT) {
                              case "dev":
                                  rds_endpoint = "app-interchange-aurora-priv-cluster-dev-cluster.cluster-cf3zxr6zcsiz.us-east-1.rds.amazonaws.com"
                                  break
                              case "prd":
                                  rds_endpoint = "app-interchange-aurora-priv-cluster-prd-cluster.cluster-cf3zxr6zcsiz.us-east-1.rds.amazonaws.com"
                                  break
                              default:
                                  rds_endpoint = ""
                          }

                          LAMBDA_APP_ARN = jsonResult.Stacks[0].Outputs.find {element -> element.OutputKey == 'LambdaFunctionName'}.OutputValue
                          OPENSEARCH_URL = jsonResult.Stacks[0].Outputs.find {element -> element.OutputKey == 'OpenSearchDomainEndpoint'}.OutputValue
                          OPENSEARCH_SECRET = jsonResult.Stacks[0].Outputs.find {element -> element.OutputKey == 'OpenSearchSecret'}.OutputValue
                          RDS_AURORA_SECRET = jsonResult.Stacks[0].Outputs.find {element -> element.OutputKey == 'RdsAuroraSecret'}.OutputValue
                          RDS_INSTANCE_ENDPOINT = rds_endpoint
                          CODE_DEPLOY_APPLICATION = "Application-app-interchange"
                          CODE_DEPLOY_DEPLOYMENT_GROUP = "Dg-app-interchange-${ENVIRONMENT}"
                    }
                }
            }
        }*/

        stage("Infrastructure Provisioning"){
            steps {
                script{
                    switch (ENVIRONMENT) {
                        case "dev":
                            LAMBDA_APP_ARN = "arn:aws:lambda:us-east-1:861276092327:function:itl-0004-itx-dev-lmbd-app-01"
                            OPENSEARCH_URL = "https://search-itl-0004-itx-dev-srch-01-ybnsgoolbtvgknoqz5qndmfsd4.us-east-1.es.amazonaws.com"
                            OPENSEARCH_SECRET = "arn:aws:secretsmanager:us-east-1:861276092327:secret:itl-0004-itx-dev-secret-opensearch-01-mZ3zDb"
                            RDS_AURORA_SECRET = "arn:aws:secretsmanager:us-east-1:861276092327:secret:itl-0004-itx-dev-secret-rds-app-01-m7jFud"
                            RDS_INSTANCE_ENDPOINT = "itl-0004-itx-dev-rds-app-01.cluster-ca5ywgwaoh7p.us-east-1.rds.amazonaws.com"
                            CODE_DEPLOY_APPLICATION = "itl-0004-itx-all-codedeploy-ec2-app-01"
                            CODE_DEPLOY_DEPLOYMENT_GROUP = "itl-0004-itx-all-codedeploy-ec2-app-dev-01-dg"
                            break
                        case "prd":
                            rds_endpoint = "app-interchange-aurora-priv-cluster-prd-cluster.cluster-cf3zxr6zcsiz.us-east-1.rds.amazonaws.com"
                            break
                        default:
                            rds_endpoint = ""
                    }
                }    
            }
        }

        stage('Deploying Lambda App') {
            steps {
                script {
                    dir('Lambdas/lambda_app'){
                        sh (returnStdout: false, script: "zip -r ${LAMBDA_APP_ZIP_NAME} *", label: "Compressing files ..." )

                        sh (returnStdout: true, script: "aws s3 cp ${LAMBDA_APP_ZIP_NAME} s3://${ARTIFACT_BUCKET}/app-interchange/builds/${ENVIRONMENT}/lambda-app/", label: "Uploading to s3")

                        def scriptString = "aws lambda update-function-code \
                                          --function-name ${LAMBDA_APP_ARN} \
                                          --s3-bucke ${ARTIFACT_BUCKET} \
                                          --s3-key app-interchange/builds/${ENVIRONMENT}/lambda-app/${LAMBDA_APP_ZIP_NAME}
                                          //--zip-file fileb://${LAMBDA_APP_ZIP_NAME}"
                        sh (returnStdout: false, script: scriptString, label: "Deploying Lambda App ..." )
                    }
                }
            }
        }

        stage('Deploying App') {
            steps {
                script {
                    def getOpenSearchSecret = sh (returnStdout: true, script: "aws secretsmanager get-secret-value --secret-id ${OPENSEARCH_SECRET} --output text --query SecretString", label: "Getting secret name ..." )
                    def jsonOSSecretResult = readJSON text: getOpenSearchSecret

                    def getRdsAuroraSecret = sh (returnStdout: true, script: "aws secretsmanager get-secret-value --secret-id ${RDS_AURORA_SECRET} --output text --query SecretString", label: "Getting secret name ..." )
                    def jsonRdsSecretResult = readJSON text: getRdsAuroraSecret

                    OPENSEARCH_USER = jsonOSSecretResult.username
                    OPENSEARCH_PASSWORD = jsonOSSecretResult.password
                    RDSAURORA_PASSWORD = jsonRdsSecretResult.password

                    wrap([$class: 'MaskPasswordsBuildWrapper',
                          varPasswordPairs: [[password: OPENSEARCH_PASSWORD],
                                             [password: RDSAURORA_PASSWORD]]]) {
                        sh 'sed -i -e "s;%DOMAIN_ENDPOINT%;' + OPENSEARCH_URL + ';g"\
                          -e "s;%USER%;' + OPENSEARCH_USER +';g" -e "s;%PASSWORD%;'+ scapeString(OPENSEARCH_PASSWORD) +';g" Config/logstash/filebeat-intelica.conf'

                        sh 'sed -i -e "s;%RDS_ENDPOINT%;' + RDS_INSTANCE_ENDPOINT + ';g"\
                            -e "s;%RDS_PASSWORD%;'+ scapeString(RDSAURORA_PASSWORD) +';g" .env'
                    }

                    sh (returnStdout: false, script: 'sed -i -e "s;%ENVIRONMENT%;' + ENVIRONMENT + ';g"\
                        ci/scripts/after_install.sh', label: "Replacing after install event...")

                    sh (returnStdout: false, script: "zip -r ${APP_ZIP_NAME} Module", label: "Compressing Module files ..." )

                    sh (returnStdout: false, script: "zip -ur ${APP_ZIP_NAME} Build", label: "Compressing Build files ..." )

                    sh (returnStdout: false, script: "zip -ur ${APP_ZIP_NAME} Config", label: "Compressing Config files ..." )

                    sh (returnStdout: false, script: "zip -ur ${APP_ZIP_NAME} Dep", label: "Compressing Dep files ..." )

                    dir('ci'){
                        sh (returnStdout: false, script: "zip -ur ../${APP_ZIP_NAME} scripts appspec.yml", label: "Compressing CodeDeploy files ..." )
                    }

                    sh (returnStdout: false, script: "zip -ur ${APP_ZIP_NAME} *.py .env", label: "Compressing python files ..." )

                    sh (returnStdout: false, script: "aws s3 cp ${APP_ZIP_NAME} s3://${ARTIFACT_BUCKET}/app-interchange/builds/${ENVIRONMENT}/ec2-app/", label: "Uploading to s3")

                    def deployment = sh (returnStdout: true, script: "aws deploy create-deployment \
                        --application-name ${CODE_DEPLOY_APPLICATION} \
                        --deployment-group-name ${CODE_DEPLOY_DEPLOYMENT_GROUP} \
                        --file-exists-behavior OVERWRITE --ignore-application-stop-failures \
                        --s3-location bucket=${ARTIFACT_BUCKET},bundleType=zip,key=app-interchange/builds/${ENVIRONMENT}/ec2-app/${APP_ZIP_NAME}", label: "Deploying App ...")

                    def jsonResult = readJSON text: deployment

                    awaitDeploymentCompletion("${jsonResult.deploymentId}")
                }
            }
        }
    }

    post {
       cleanup {
           cleanWs()
       }
     }
}

def getStackStatus(String stackName){
    def stackStatus

    try {
        def getStackStatus = sh (returnStdout: true, script: "aws cloudformation describe-stacks --stack-name ${stackName}", label: "Describe ${stackName} Stack").trim()
        def jsonResult = readJSON text: getStackStatus

        stackStatus = jsonResult.Stacks[0].StackStatus

    } catch (err) {
        echo "Stack No existe"
    }

    return stackStatus
}

def readYamlParameters(){

    def configValues = readYaml file: 'parameters.yaml'

    def stringParameterOverrides = "Environment=${ENVIRONMENT}"

    if(configValues[ENVIRONMENT] != null){

      configValues[ENVIRONMENT].each{
          key, value ->
          stringParameterOverrides = stringParameterOverrides + " ${key}=${value}"
      }
    }

    return stringParameterOverrides
}

def getEnvironment()
{
      def environment = 'test'
      def (vcs, org, project) = "${env.GIT_URL}".split('//')[-1].split('/')

      if(vcs == 'github.com' && org == 'MillennialV' && isMain()){
        environment = 'dev'
      }

      if(vcs == 'github.com'/* && org == 'Intelica-Interchange'*/){
          if (isDevelop()) {
            environment = 'dev'
          }

          if (isPre()) {
            environment = 'qas'
          }

          if (isMain()) {
            environment = 'prd'
          }
      }

      return environment
}

def isMain() {
  return env.BRANCH_NAME == 'main'
}

def isDevelop() {
  return env.BRANCH_NAME == 'dev'
}

def isPre() {
  return env.BRANCH_NAME == 'pre'
}

def getShortCommitId() {
    def gitCommit = env.GIT_COMMIT
    def shortGitCommit = "${gitCommit[0..6]}"
    return shortGitCommit
}

def scapeString(String s){
    return s.replaceAll(/\|/, "\\\\|")
    .replaceAll(/&/, "\\\\&")
    .replaceAll(/;/, "\\\\;")
    .replaceAll(/</, "\\\\<")
    .replaceAll(/>/, "\\\\>")
    .replaceAll(/\(/, "\\\\(")
    .replaceAll(/\)/, "\\\\)")
    .replaceAll(/\$/, "\\\\\$")
    .replaceAll(/`/, "\\\\`")
    .replaceAll(/"/, "\\\\\"")
    .replaceAll(/'/, "\\\\'")
}
