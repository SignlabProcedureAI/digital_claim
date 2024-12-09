
# 모듈: 메일(텍스트) 자동화 전처리 


# basic
import pandas as pd
import warnings

# time
from dateutil import parser

def read_mail_text(file_path):
    """ 메일 → 텍스트 변환 
     메일 텍스트 파일을 읽어 데이터프레임으로 변환하는 함수.
    
    Args: file_path: 메일 텍스트 경로

    Returns: 메일 텍스트 데이터 프레임 
    """
    
    # 파일 읽기
    with open(file_path, 'r', encoding='cp949') as file:
        lines = file.readlines()

    # 줄 바꿈 문자 제거
    lines = [line.strip() for line in lines]

    # 데이터 프레임 생성
    df = pd.DataFrame(lines, columns=['POC_TEXT'])
    
    return df



def extract_text_ranges(data):

    """
    텍스트 데이터에서 조건에 맞는 시작과 끝 인덱스 쌍을 추출하는 함수.

    Args:
        data (pd.DataFrame): "POC_TEXT" 열을 포함한 데이터프레임.

    Returns:
        list of tuple: 각 튜플은 (start_index, end_index) 형태로, 
                       "From:"으로 시작하고 "regards"로 끝나는 범위를 나타냄.
    """

    # "보낸 사람"으로 시작하는 행과 "regards"로 끝나는 행의 인덱스를 찾기
    start_indices = data[data['POC_TEXT'].str.contains("^[\s]*From:", regex=True)].index
    end_indices = data[(data['POC_TEXT'].str.contains("regards"))| (data['POC_TEXT'].str.contains("Regards"))].index
    
    # end 리스트를 위한 반복자 생성
    end_iter = iter(end_indices)

    # 결과를 저장할 리스트
    result = []
    
    # 현재 end 인덱스를 가져오기 위한 변수, 초기값은 반복자의 첫 번째 값
    current_end = next(end_iter, None)

    used_start_indices = set()  # 이미 사용된 end 값을 추적하기 위한 집합
    
    # 모든 start 인덱스에 대해 반복
    for start in start_indices:
        if start in used_start_indices:  # 이미 사용된 start 인덱스는 건너뛰기
            continue
            
        # 현재 end 인덱스가 start 인덱스보다 작거나 같으면, 다음 end 인덱스로 이동
        while current_end is not None and current_end <= start:
            current_end = next(end_iter, None)
            
        # 유효한 end 인덱스 찾기 (start 인덱스보다 큰 첫 번째 end 인덱스)
        if current_end is not None and current_end > start:
            result.append((start, current_end))
            used_start_indices.add(start)  # Mark the current start index as used
            
    return result

def extract_email_account(email_data):
    """ Sender와 Recipient의 메일 분리
     Args: 
         email_data: 이메일 데이터 프레임 
     
     Returns:
         Sender, Email 분리 후 데이터 프레임
    """
    email_data['Sender_Email'] = 0
    email_data['Recipient_Email'] = 0
    
    for index, rows in email_data.iterrows():
#         print(f"\n [INFO]: 데이터 출력 : \n {rows}")

        sender_list = rows['Sender'].split('<')
        recipient_list = rows['Recipient'].split('<')
    
        # Sender와 Recipient 데이터 분리 후 공백 제거 후 대입
        email_data.loc[index,'Sender'] = sender_list[0].strip()
        email_data.loc[index,'Recipient'] = recipient_list[0].strip()
        
        # Sender와 Recipient 이메일 부분만 파싱
        email_data.loc[index,'Sender_Email'] = sender_list[1].replace('>','')
        email_data.loc[index,'Recipient_Email'] = recipient_list[1].replace('>','')
    
    return email_data 

def  extract_email_info(data):
    """
    이메일 형식의 텍스트 데이터를 파싱하여 주요 정보를 추출하는 함수.

    Args:
        data (pd.DataFrame): "POC_TEXT" 열을 포함한 데이터프레임.

    Returns:
        tuple: (보낸 사람, 받는 사람, 날짜, 제목, 본문)의 데이터를 포함하는 튜플.
    """
    
    # "보낸 사람"으로 시작하는 행과 "regards"로 끝나는 행의 인덱스를 찾기
    from_indices = data[data['POC_TEXT'].str.contains("^[\s]*From:", regex=True)].index[0]
    to_indices = data[data['POC_TEXT'].str.contains("To")].index[0]
    date_indices = data[(data['POC_TEXT'].str.contains("Sent")) | (data['POC_TEXT'].str.contains("Date"))].index[0]
    subject_indices = data[data['POC_TEXT'].str.contains("Subject") | (data['POC_TEXT'].str.contains("subject"))].index[0]
    message_indices = subject_indices + 2

    sender = data.loc[from_indices]['POC_TEXT'].split(':')[1]  # "보낸 사람"에서 이름 추출
    recipient = data.loc[to_indices]['POC_TEXT'].split(':')[1]  # "보낸 사람"에서 이름 추출
    date = data.loc[date_indices]['POC_TEXT'].split(': ')[1]  # "날짜"에서 날짜 추출
    subject = data.loc[subject_indices]['POC_TEXT'].split(':')[1:]  # "날짜"에서 날짜 추출
    
    # subject 결합
    combined_subject = " ".join(subject)
    
    message = data.loc[message_indices:]['POC_TEXT']  # "메시지 내용"에서 내용 추출

    # 메시지 결합
    combined_message= " ".join(message)
    
    
    return sender,recipient,date,combined_subject,combined_message



def generate_email_dataframe(data):
    """
    정제된 이메일 정보를 데이터프레임으로 변환하는 함수.

    Args:
        data (pd.DataFrame): 이메일 텍스트 데이터를 포함한 데이터프레임.

    Returns:
        pd.DataFrame: 추출된 이메일 정보('Sender', 'Recipient', 'Date', 'Subject', 'Message')로 구성된 데이터프레임.
    """
     
    # 추출된 데이터를 저장할 새로운 데이터 프레임 생성
    extracted_df = pd.DataFrame(columns=['Sender','Recipient','Date','Subject','Message'])
    
    # 각 내용 추출 함수
    sender,recipient,date,combined_subject,combined_message = extract_email_info(data)

    # 추출된 데이터를 새로운 데이터 프레임에 추가
    new_row = pd.DataFrame([{
        'Sender': sender,
        'Recipient': recipient,
        'Date': date,
        'Subject': combined_subject,
        'Message': combined_message
    }])
    
    
    # 추출된 데이터를 새로운 데이터 프레임에 추가
    extracted_df = pd.concat([extracted_df, new_row], ignore_index=True)

    return extracted_df



def apply_fuc_email_dataframe(file_path): 
    
    # 텍스트 파일 함수 사용
    mail_data = read_mail_text(file_path)
    
    # Zip 생성 함수 사용
    condition_zip = extract_text_ranges(mail_data)

    # 리스트 생성
    poc_bowl=[]

    # 각 섹션의 텍스트 추출
    for start, end in condition_zip:

        section = mail_data.loc[start:end]
        
        refined_section = generate_email_dataframe(section)
    
        poc_bowl.append(refined_section)
    
    prep_data = pd.concat(poc_bowl)
    
    prep_data = prep_data.reset_index(drop=True)

    # Sender와 Recipient의 메일 분리
    prep_data = extract_email_account(prep_data)

    # 시간 파싱 후 정제
    prep_data['parsed_date'] = prep_data['Date'].apply(parser.parse)
    prep_data['parsed_date'] = pd.to_datetime(prep_data['parsed_date'],utc=True)

    return prep_data

