import logging
import asyncio
import random
import time
import os
import json
import unicodedata
import re
import io
import difflib
import requests
import httpx
import base64
import logging
import datetime  
import aiohttp
import arabic_reshaper
from datetime import datetime, timedelta # 💡 تمت الإضافة هنا
from aiogram import types
from aiogram.dispatcher.filters import Text 
from pilmoji import Pilmoji 
from PIL import Image, ImageDraw, ImageFont, ImageOps
from bidi.algorithm import get_display
from aiogram import Bot, Dispatcher, types, executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from supabase import create_client, Client

# --- [ 1. إعدادات الهوية والاتصال ] ---
# --- [ 1. إعدادات الهوية والاتصال - نسخة أثير المطور ] ---
# جلب آيدي المطور من ريندر (قيمة افتراضية إذا لم يوجد)
ADMIN_ID = int(os.getenv('ADMIN_ID', 7988144062)) 
OWNER_USERNAME = os.getenv('OWNER_USERNAME', "@Ya_79k")

# --- [ 3. سحب التوكينات والقلوب الثلاثة ] ---
# --- [ 2. تعريف الهوية والمسارات ] ---
LOG_GROUP_ID = os.getenv('LOG_GROUP_ID')
API_TOKEN = os.getenv('BOT_TOKEN')
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

# مفاتيح GROQ البرمجية
GROQ_KEYS = [
    os.getenv('G_KEY_1'),
    os.getenv('G_KEY_2'),
    os.getenv('G_KEY_3')
]

# رسالة تأكيد في اللوج عند بدء التشغيل (اختياري)
if LOG_GROUP_ID:
    logging.info(f"✅ رادار الأخطاء جاهز للعمل على الوجهة المشفرة.")
    

# تصفية المصفوفة لضمان عدم وجود قيم فارغة
GROQ_KEYS = [k for k in GROQ_KEYS if k]
current_key_index = 0  # مؤشر تدوير القلوب

# التحقق من وجود المتغيرات الأساسية لضمان عدم حدوث Crash
if not API_TOKEN or not GROQ_KEYS:
    logging.error("❌ خطأ: المتغيرات المشفرة مفقودة في إعدادات Render!")

# تعريف المحركات
bot = Bot(token=API_TOKEN, parse_mode="HTML")
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# مخزن مؤقت لربط الـ Polls بالأسئلة (يعمل كـ الرام السريع)
active_polls = {}

active_quizzes = {}
cancelled_groups = set() # لحفظ المجموعات التي ضغطت إلغاء مؤقتاً
# في أعلى الملف تماماً (Global Variable)
answered_users_global = {}

# 📋 قاموس تعقب المهام الجارية (يوضع في أعلى الملف)
quiz_tasks = {}
overall_scores = {}

async def send_log(error_type, error_details, chat_id=None, user_id=None):
        """
        إرسال تقرير خطأ مفصل إلى الوجهة المشفرة (LOG_GROUP_ID)
        """
        try:
                if not LOG_GROUP_ID:
                        logging.error("⚠️ LOG_GROUP_ID غير معرف. تأكد من إعدادات ريندر.")
                        return

                # التصحيح المباشر للوقت
                import datetime as dt
                now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                report_msg = (
                        f"🚀 **تقرير رادار الأخطاء**\n"
                        f"━━━━━━━━━━━━━━━\n"
                        f"📅 **الوقت:** `{now}`\n"
                        f"🚨 **النوع:** `{error_type}`\n"
                        f"📍 **الموقع:** `{chat_id if chat_id else 'نظام داخلي'}`\n"
                        f"👤 **المستخدم:** `{user_id if user_id else 'غير معروف'}`\n"
                        f"━━━━━━━━━━━━━━━\n"
                        f"🔍 **التفاصيل:**\n"
                        f"`{error_details}`"
                )

                await bot.send_message(LOG_GROUP_ID, report_msg, parse_mode="Markdown")
                
        except Exception as e:
                logging.error(f"❌ فشل إرسال التقرير للرادار: {e}")


   # ==========================================
# 🧹 دالة المنظف الآلي (تحذف البيانات بعد دقيقة واحدة)
# ==========================================
# كود تنظيف آمن (اختياري)
async def safe_database_cleaner():
    while True:
        try:
            # حذف فقط المسابقات اللي مر عليها ساعة كاملة (60 دقيقة)
            # لأن مستحيل مسابقة تظل ساعة كاملة شغالة
            cutoff_time = (datetime.utcnow() - timedelta(hours=1)).isoformat()
            supabase.table("active_quizzes").delete().lt("created_at", cutoff_time).execute()
        except: pass
        await asyncio.sleep(3600) # يفحص كل ساعة مرة واحدة فقط
# ==========================================
# 4. محركات العرض والقوالب (Display Engines) - النسخة المصلحة
# ==========================================
# [3] دالة قالب السؤال (المصلحة)
async def send_quiz_question(chat_id, q_data, current_num, total_num, settings):
    is_pub = settings.get('is_public', False) 
    q_scope = "إذاعة عامة 🌐" if is_pub else "مسابقة داخلية 📍"
    q_mode = settings.get('mode', 'السرعة ⚡')
    is_hint_on = settings.get('smart_hint', False) # الزر المفعل قبل الحفظ
    
    # استخراج التلميح العادي (البنيوي)
    normal_hint = settings.get('normal_hint', "")

    if q_data.get('bot_category_id'):
        real_source = "أسئلة البوت 🤖"
    elif q_data.get('user_id') or 'answer_text' in q_data:
        real_source = "أسئلة الأعضاء 👥"
    else:
        real_source = "أقسام خاصة 🔒"

    # جلب نص السؤال وتجهيزه للتنسيق الجديد
    q_text = q_data.get('question_content') or q_data.get('question_text') or "⚠️ نص السؤال مفقود!"
    
    # 🎭 صياغة الرسالة باستخدام وسوم HTML لضمان الفخامة
    text = (
        f"🎓 <b>الـمنـظـم:</b> {settings['owner_name']} ☁️\n"
        f"  ❃┅┅┅┄┄┄┈•❃•┈┄┄┄┅┅┅❃\n"
        f"📌 <b>السؤال:</b> « {current_num} » من « {total_num} »\n"
        f"📂 <b>القسم:</b> <code>{settings['cat_name']}</code>\n"
        f"🛠 <b>المصدر:</b> <code>{real_source}</code>\n"
        f"📡 <b>النطاق:</b> <b>{q_scope}</b>\n"
        f"🔖 <b>النظام:</b> {q_mode}\n"
        f"⏳ <b>المهلة:</b> {settings['time_limit']} ثانية\n"
        f"  ❃┅┅┅┄┄┄┈•❃•┈┄┄┄┅┅┅❃\n\n"
        f"❓ <b>السؤال:</b>\n<b>{q_text}</b>\n"
    )
    
    # 💡 [ عرض التلميح الماسي المطور ]
    if is_hint_on and normal_hint:
        # فاصل أنيق بين السؤال والتلميح
        text += f"\n──────────────\n{normal_hint}"

    try:
        # ⚠️ الإرسال بنظام HTML حصراً ليدعم التشفير والبعثرة والاقتباسات
        return await bot.send_message(chat_id, text, parse_mode='HTML')
    except Exception as e:
        logging.error(f"⚠️ خطأ في تنسيق HTML للمايسترو: {e}")
        # في حال حدوث خطأ، نقوم بتنظيف النص من أي وسوم وإرساله كخطة بديلة (Backup)
        import re
        clean_text = re.sub('<[^<]+?>', '', text)
        return await bot.send_message(chat_id, clean_text)
# ==========================================
# 4. دالة قالب السؤال (نظام الـ Poll الهجين)
# ==========================================

def get_hybrid_poll_style(q_data, current_index, total_q, cat_name):
    """
    تجهيز نص السؤال ليكون مطابقاً لشكل @QuizBot ولكن بنظام الاستفتاء
    """
    # 1. استخراج نص السؤال
    question_text = q_data.get('question_content') or q_data.get('question_text') or "⚠️ سؤال بدون نص"
    
    # 2. بناء "رأس السؤال" (Header)
    # ملاحظة: الـ Poll في عنوانه لا يدعم HTML، لذا نستخدم التنسيق النصي المرتب
    # السطر الأول: الترقيم (مثلاً [1 من 10])
    # السطر الثاني: القسم
    # السطر الثالث: نص السؤال
    poll_title = (
        f"[{current_index} من {total_q}]\n"
        f"اختبار: {cat_name}\n\n"
        f"❓ {question_text}"
    )
    
    return poll_title

# ==========================================
# 5. دالة المايسترو (بنظام الـ Poll الهجين)
# ==========================================
async def send_quiz_master(chat_id, q_data, current_num, total_num, settings, all_questions_list):
    try:
        style = settings.get('quiz_style', 'اختيارات 📊')
        quiz_db_id = settings.get('quiz_db_id') # هذا هو ID المسابقة في سوبابيس
        
        raw_q_text = str(q_data.get('question_content', "")).strip()
        correct_ans = str(q_data.get('correct_answer', "")).strip()
        cat_name = settings.get('cat_name', 'عام')

        # --- [ نمط الاختيارات الذكي ] ---
        if style == "اختيارات 📊":
            # 1. تجهيز الخيارات بالرادار
            clean_q_text = re.sub(r'[؟!؟\.،,:]', '', raw_q_text)
            wrong_picks = await get_ultra_smart_options(clean_q_text, cat_name, correct_ans)
            
            final_options = list(wrong_picks) + [correct_ans]
            random.shuffle(final_options) 
            correct_id = final_options.index(correct_ans)

            # 2. بناء نص السؤال المنسق
            poll_title = f"[{current_num} من {total_num}]\nاختبار: {cat_name}\n\n❓ {raw_q_text}"

            # 3. إرسال الـ Poll الفعلي
            quiz_msg = await bot.send_poll(
                chat_id=chat_id,
                question=poll_title,
                options=final_options,
                type='quiz',
                correct_option_id=correct_id,
                is_anonymous=False, # ضروري جداً ليعرف البوت من الشخص الذي أجاب
                explanation=f"✅ الإجابة الصحيحة هي: {correct_ans}"
            )

            # 🔥 [ الوصلة الذهبية ] - توضع هنا مباشرة بعد الإرسال الناجح
            # نستخدم poll.id كمفتاح لأنه الوحيد الذي يصلنا في الـ Handler
            active_polls[quiz_msg.poll.id] = {
                "db_quiz_id": quiz_db_id,      # للربط بجدول active_quizzes
                "chat_id": chat_id,           # لمعرفة المجموعة التي جاءت منها الإجابة
                "category": cat_name,         # لتخزين القسم في answers_log
                "correct_id": correct_id,     # لمقارنة إجابة اللاعب
                "correct_text": correct_ans,   # لحفظ نص الإجابة
                "current_num": current_num,   # رقم السؤال الحالي
                "total_num": total_num,       # إجمالي الأسئلة
                "start_time": datetime.now(), # لبدء عداد السرعة (الملي ثانية)
                "q_id": q_data.get('id')      # آيدي السؤال من قاعدة البيانات
            }
            
            print(f"🚀 [رادار]: تم تفعيل مراقبة السؤال {current_num} للـ Poll: {quiz_msg.poll.id}")
            return quiz_msg

        # --- [ الأنماط الأخرى: مباشر / كتابة ] ---
        else:
            return await send_quiz_question(chat_id, q_data, current_num, total_num, settings)

    except Exception as e:
        print(f"❌ Error in Master Engine: {e}")
        return await send_quiz_question(chat_id, q_data, current_num, total_num, settings)
# ==========================================
# --- [ دالة تسجيل الإجابة في سوبابيس المحدثة ] ---
# ==========================================
async def record_poll_answer_in_db(answer_data):
    """
    تستقبل مصفوفة البيانات الجاهزة وترسلها لجدول answers_log
    """
    try:
        # 1. تنفيذ الإرسال لجدول answers_log (الاسم الجديد)
        # نحن نستخدم الإدراج (insert) لإضافة سجل جديد لكل إجابة
        res = supabase.table("answers_log").insert(answer_data).execute()
        
        # 2. فحص بسيط للتأكد من نجاح العملية (اختياري)
        if hasattr(res, 'data') and len(res.data) > 0:
            user = answer_data.get('user_name', 'مجهول')
            status = "✅ تم الحفظ"
            print(f"🚀 [DB]: تم تسجيل إجابة {user} بنجاح في سجل الإجابة.")
        else:
            print(f"⚠️ تنبيه: تم إرسال البيانات ولكن لم يتم التأكد من الحفظ.")

    except Exception as e:
        # في حال وجود خطأ (مثلاً آيدي غير موجود أو مشكلة في الاتصال)
        print(f"❌ فشل تسجيل الإجابة في answers_log: {e}")

# ==========================================
# ==========================================
# --- [دالة مساعدة لتوحيد النصوص ] ---
# ==========================================
def normalize_arabic(text):
    if not text: return ""
    text = str(text).strip()
    # توحيد الألفات، التاء المربوطة، الياء، وحذف التشكيل
    text = re.sub(r'[أإآ]', 'ا', text)
    text = re.sub(r'ة', 'ه', text)
    text = re.sub(r'ى', 'ي', text)
    text = re.sub(r'[\u064B-\u0652]', '', text)
    return text
# ==========================================
# --- [ المحرك الخارق: الرادار الذكي والقوافي ] ---
# ==========================================
async def get_ultra_smart_options(question_text, category_name, correct_ans):
    try:
        norm_correct = normalize_arabic(correct_ans)
        fakes = []
        seen_norms = {norm_correct}
        
        ans_words = correct_ans.split()
        q_norm = normalize_arabic(question_text)

        # 1️⃣ [ مصفوفة الموسوعة العالمية الشاملة - الإصدار النهائي المطلق 2026 ] 🌐
        # تم تصميمها لامتصاص كل أنواع الأسئلة على وجه الأرض مع معالجة (هـ / ة)
        patterns = {
            # --- التاريخ والزمن والحروب ---
            'history_war': [
                'عام', 'سنه', 'سنة', 'تاريخ', 'قرن', 'ميلادي', 'هجري', 'معركه', 'معركة', 
                'غزوه', 'غزوة', 'ثوره', 'ثورة', 'حضاره', 'حضارة', 'امبراطور', 'ملك', 'سلالة', 'عصر'
            ],
            
            # --- الجغرافيا والكون والطبيعة ---
            'geography_world': [
                'دوله', 'دولة', 'بلد', 'عاصمه', 'عاصمة', 'مدينه', 'مدينة', 'نهر', 'بحر', 
                'محيط', 'جبل', 'قاره', 'قارة', 'جزيره', 'جزيرة', 'خليج', 'مضيق', 'مناخ', 'تضاريس', 'غابة'
            ],
            
            # --- العلوم والتقنية والطب (التشريح الدقيق) ---
            'science_tech': [
                'كوكب', 'عنصر', 'غاز', 'حيوان', 'طائر', 'نبات', 'جسم', 'خليه', 'خلية', 
                'جهاز', 'مخترع', 'اكتشف', 'فيزياء', 'كيمياء', 'مجره', 'مجرة', 'فضاء', 'تلسكوب', 'هندسة'
            ],
            'medical_health': [
                'عضو', 'مرض', 'دواء', 'فيتامين', 'هرمون', 'عصب', 'عظم', 'دم', 'قلب', 
                'دماغ', 'عضله', 'عضلة', 'غده', 'غدة', 'فيروس', 'بكتيريا', 'علاج'
            ],
            
            # --- الفكر والأدب والشخصيات ---
            'humanities': [
                'من هو', 'الشاعر', 'مؤلف', 'كاتب', 'فنان', 'رسام', 'فيلسوف', 'رواية', 
                'قصيدة', 'لوحة', 'تمثال', 'مسرحية', 'فكر', 'منطق', 'ثقافة'
            ],
            'islamic_religion': [
                'سوره', 'سورة', 'آيه', 'آية', 'نبي', 'رسول', 'الصحابي', 'تابعي', 'فقيه', 
                'كتاب', 'تفسير', 'حديث', 'صلاه', 'صلاة', 'حج', 'زكاه', 'زكاة', 'غزوة'
            ],
            
            # --- الرياضة والترفيه والألعاب ---
            'sports_ent': [
                'لاعب', 'نادي', 'منتخب', 'بطوله', 'بطولة', 'كاس', 'كأس', 'ملعب', 'هدف', 
                'اولمبياد', 'فيلم', 'مسلسل', 'ممثل', 'مخرج', 'اغنية', 'اغنيه', 'رقم قياسي'
            ],
            
            # --- الحساب والمنطق والقياسات ---
            'math_logic': [
                'حساب', 'رياضيات', 'مربع', 'مثلث', 'زاويه', 'زاوية', 'مجموع', 'ناتج', 
                'معادله', 'معادلة', 'رقم', 'ضعف', 'جذر', 'قسمة', 'ضرب'
            ],
            'measurements': [
                'كم عدد', 'كم يبلغ', 'ما طول', 'ما وزن', 'نسبه', 'نسبة', 'مسافه', 'مسافة', 
                'الوحده', 'الوحدة', 'لقياس', 'مادة', 'مادة', 'الزمن', 'سرعه', 'سرعة', 'درجه', 'درجة'
            ],
            
            # --- الاقتصاد، القانون، والسياسة ---
            'eco_law_pol': [
                'عملة', 'عمله', 'بورصة', 'اقتصاد', 'شركة', 'شركه', 'قانون', 'محكمة', 
                'دستور', 'رئيس', 'وزير', 'برلمان', 'انتخابات', 'سياسة'
            ],

            # --- اللغة، الأمثال، الحياة اليومية ---
            'language_proverbs': [
                'مرادف', 'ضد', 'معنى', 'جمع', 'مفرد', 'مثل', 'حكمه', 'حكمة', 'قائل', 
                'كلمة', 'كلمه', 'لغة', 'لغه', 'لهجة', 'لهجه'
            ],
            'lifestyle_brands': [
                'اكله', 'اكلة', 'وجبه', 'وجبة', 'مطبخ', 'فاكهه', 'فاكهة', 'خضار', 
                'سياره', 'سيارة', 'ماركه', 'ماركة', 'شعار', 'هاتف', 'تطبيق', 'برج'
            ],
            
            # --- الأساطير والماورائيات ---
            'myth_folklore': [
                'اسطورة', 'اسطوره', 'خرافة', 'خرافه', 'اله', 'عملاق', 'تنين', 'فلكلور'
            ]
        }
        
        detected_pattern = None
        for key, words in patterns.items():
            if any(w in q_norm for w in words):
                detected_pattern = key
                break

        # 2️⃣ نظام الصيد بالليزر (Strict Type Logic)
        if detected_pattern:
            search_keywords = patterns[detected_pattern]
            # ذكاء اصطناعي مصغر: استخراج الكلمة النوعية مع إزالة الـ التعريف
            specific_type = next((w for w in search_keywords if w in q_norm), None)
            
            query = supabase.table("bot_questions").select("correct_answer")
            
            if specific_type:
                # إزالة "ال" التعريف للبحث الشامل
                clean_type = re.sub(r'^ال', '', specific_type)
                res = query.ilike("question_content", f"%{clean_type}%").limit(60).execute()
            else:
                or_filter = ",".join([f"question_content.ilike.%{w}%" for w in search_keywords])
                res = query.or_(or_filter).limit(60).execute()
            
            if res.data:
                for r in res.data:
                    opt = str(r['correct_answer']).strip()
                    if normalize_arabic(opt) not in seen_norms:
                        # حماية الأرقام
                        if any(char.isdigit() for char in correct_ans) and not any(char.isdigit() for char in opt):
                            continue
                        # تناسق الطول
                        if abs(len(opt) - len(correct_ans)) <= 12:
                            fakes.append(opt)
                            seen_norms.add(normalize_arabic(opt))
                
                if len(fakes) >= 3: 
                    return random.sample(fakes, 3)

        # 3️⃣ نظام "الكلمة الأولى" (البحث عن نفس العائلة اللفظية)
        if len(ans_words) >= 1:
            first_word = re.sub(r'^ال', '', ans_words[0]) # إزالة ال التعريف من الإجابة أيضاً
            if len(first_word) > 2:
                res = supabase.table("bot_questions").select("correct_answer") \
                    .ilike("correct_answer", f"%{first_word}%") \
                    .limit(20).execute()
                
                for r in res.data:
                    opt = str(r['correct_answer']).strip()
                    if normalize_arabic(opt) not in seen_norms:
                        fakes.append(opt)
                        seen_norms.add(normalize_arabic(opt))
                
                if len(fakes) >= 3: return random.sample(fakes, 3)

        # 4️⃣ نظام "التطابق الشكلي" (لضمان وجود خيارات دائماً)
        if len(fakes) < 3:
            # زيادة البحث في القسم ليشمل خيارات أكثر
            res = supabase.table("bot_questions").select("correct_answer") \
                .eq("category", category_name).limit(50).execute()
            
            for r in res.data:
                opt = str(r['correct_answer']).strip()
                if normalize_arabic(opt) not in seen_norms:
                    # تفضيل نفس عدد الكلمات
                    if len(opt.split()) == len(ans_words):
                        fakes.append(opt)
                        seen_norms.add(normalize_arabic(opt))

        # 5️⃣ التغطية النهائية (Fallback Safe)
        # نستخدم min لضمان عدم حدوث خطأ إذا كانت القائمة صغيرة
        return random.sample(fakes, min(len(fakes), 3))

    except Exception as e:
        print(f"❌ خطأ في المحرك الموسوعي: {e}")
        return []
      
# ==========================================
# 6. دالة الإرسال النهائية (الهجين الذكي)
# ==========================================
async def send_hybrid_poll_to_chat(chat_id, title, options, correct_id, correct_text, q_id):
    """
    إرسال الـ Poll الفعلي وتسجيله في الرام للرصد اللحظي
    """
    try:
        # 🚀 1. إرسال الاستفتاء الرسمي لتليجرام
        quiz_msg = await bot.send_poll(
            chat_id=chat_id,
            question=title,           # النص المنسق (رقم السؤال + القسم + السؤال)
            options=options,         # قائمة الخيارات (المغناطيس الراداري)
            type='quiz',             # وضع الاختبار (إجابة واحدة صحيحة)
            correct_option_id=correct_id, # موقع الإجابة الصحيحة
            is_anonymous=False,      # ضروري جداً لرصد "من سبق لبق"
            explanation=f"✅ الإجابة الصحيحة هي: {correct_text}", # تظهر للمخطئ
            explanation_parse_mode='HTML'
        )

        # 🚀 2. تسجيل الـ Poll في "ذاكرة الرصد السريعة" (الرام)
        # نربط معرف الـ Poll (poll.id) ببيانات السؤال لكي نعرفه عندما يضغط اللاعب
        active_polls[quiz_msg.poll.id] = {
            "q_id": q_id,
            "correct_id": correct_id,
            "correct_text": correct_text,
            "start_time": datetime.now() # لحساب سرعة الإجابة بالملي ثانية
        }
        
        return quiz_msg

    except Exception as e:
        print(f"❌ فشل إرسال الـ Poll الهجين: {e}")
        return None
# ==========================================
# --- [ 2. بداية الدوال المساعدة قالب الاجابات  ] ---
# ==========================================
import re
import unicodedata

def atomic_cleaner(text):
    """
    1. محرك التقشير الذري:
    يفكك زخارف الجوكر والرموز الملتصقة ليعيد الحرف لأصله الخام.
    """
    if not text: return ""
    
    # تفكيك الحروف المزخرفة (مثل فك الارتباط بين السين والجوكر الملتصق بها)
    normalized = unicodedata.normalize('NFKD', text)
    
    # إبقاء الحروف العربية واللاتينية فقط وحذف كل رموز الجوكر (النجوم، الرموز التاريخية، الإيموجي)
    # المدى \u0600-\u06FF يغطي الحروف العربية الصافية
    clean = re.sub(r'[^\u0600-\u06FF a-zA-Z]', '', normalized)
    
    # حذف "التطويل" (ـ) الذي يستخدم في التمطيط مثل ســـــمر
    clean = clean.replace('ـ', '')
    
    # توحيد المسافات
    return " ".join(clean.split()).strip()

async def deep_privacy_scan(user, bot):
    """
    الماسح الليزري لحماية الخصوصية بنظام التشظي وإزالة الجوكر.
    """
    if not user: return False

    # --- [ جلب وتطهير البيانات ] ---
    raw_name = f"{user.first_name or ''} {user.last_name or ''}"
    username = (user.username or "").lower()
    
    bio = ""
    try:
        chat_full = await bot.get_chat(user.id)
        bio = (chat_full.bio or "").lower()
    except: pass

    # تطهير ليزري للاسم والبيو
    pure_name = atomic_cleaner(raw_name)
    pure_bio = atomic_cleaner(bio)
    
    # نظام التشظي (دمج الحروف المتباعدة مثل 'ر ي م')
    fragmented_name = pure_name.replace(" ", "")

    # --- [ الدرع الأول: قائمة المستثنيات الذكورية ] ---
    # أسماء ذكورية تنتهي بنهايات أنثوية (لمنع حظر الرجال بالخطأ)
    male_exceptions = [
        'حمزة', 'طلحة', 'عتبة', 'قتيبة', 'حذيفة', 'أسامة', 'خليفة', 
        'جمعة', 'يحيى', 'زكريا', 'علاء', 'البراء', 'طه'
    ]
    if any(ex in pure_name for ex in male_exceptions):
        return False

    # --- [ الدرع الثاني: رادار النهايات العربية ] ---
    # يه، ية، ة، وة، يا، وه، اي، ات، ى
    arabic_suffixes = r'.*(يه|ية|ة|وة|يا|وه|اي|ات|ى)$'
    if re.search(arabic_suffixes, pure_name):
        return True

    # --- [ الدرع الثالث: كلمات دلالية (يمنية وعامة) ] ---
    female_keywords = [
        'بنت', 'ام ', 'الانسة', 'الاخت', 'مدام', 'دكتورة', 'امة', 
        'خريجة', 'طالبة', 'متزوجة', 'مخطوبة', 'يمنية', 'صنعانية', 
        'عدنية', 'يافعية', 'تعزية', 'حضرمية', 'فراشة', 'وردة', 
        'زهرة', 'ياسمينة', 'غيمة', 'لؤلؤة', 'اميرة', 'ملكة', 'كيوت'
    ]
    
    combined_content = f"{pure_name} {fragmented_name} {pure_bio} {username}"
    if any(kw in combined_content for kw in female_keywords):
        return True

    # --- [ الدرع الرابع: القائمة الذهبية لأسماء البنات ] ---
    # تشمل الأسماء الصريحة التي قد لا تنتهي بـ "ة"
    feminine_list = [
        'مريم', 'العنود', 'زينب', 'حنان', 'امل', 'عبير', 'ريم', 'روان', 
        'شهد', 'رهف', 'خلود', 'دلال', 'نجلاء', 'غيداء', 'جواهر', 'هناء', 
        'وفاء', 'سمر', 'سيمان', 'هدهد', 'نور', 'تغريد', 'افنان'
    ]
    
    if any(fn in fragmented_name for fn in feminine_list):
        return True

    # --- [ الدرع الخامس: النهايات اللاتينية (Aisha, Fatima) ] ---
    # فحص النهايات اللاتينية مع استثناء (Abdullah)
    if re.search(r'(ah|ia|ya|ina|line)$', pure_name.lower()) or \
       re.search(r'(ah|ia|ya|ina|line)$', username):
        # استثناء الأسماء التي تنتهي بلفظ الجلالة باللاتينية
        if not pure_name.lower().endswith(('ullah', 'allah')):
            return True

    return False
    
 

async def send_creative_results(chat_id, correct_ans, winners, group_scores, is_public=False, mode="السرعة ⚡", group_names=None, losers=None):
    """
    🎁 نسخة الهدية - قالب ياسر الملكي (التطوير النهائي 2026)
    تم دمج درع حماية الخصوصية "الماسح الليزري" مع الحفاظ على التصميم الفخم.
    """
    mode_icon = "⚡" if "سرعة" in mode else "⏰"
    
    msg = f"🏆 <b>تـفـاصـيـل الـجـولـة الـمـلـكـيـة</b> {mode_icon}\n"
    msg += "  ━━━━━━━━━━━━━━━━━━\n"
    msg += f"🎯 الإجابة: <b>「 {correct_ans} 」</b>\n"
    msg += "  ━━━━━━━━━━━━━━━━━━\n\n"

    # --- [ 1. عرض الأبطال (الناجحين) ] ---
    if winners:
        msg += "🌟 <b>نجوم الجولة الحالية:</b>\n"
        
        for idx, w in enumerate(winners):
            medal = "🥇" if idx == 0 else "🥈" if idx == 1 else "🥉" if idx == 2 else "🏅" if idx == 3 else "🎖"
            
            raw_name = w.get('name', 'لاعب مجهول')
            u_id = w.get('id')
            u_user = w.get('user_name') # اليوزر من قاعدة البيانات
            u_home = str(w.get('home_cid', ''))
            time_val = w.get('time', 0.0)
            pts = w.get('pts', 0)
            current_chat = str(chat_id)

            # تنظيف اليوزر
            if u_user: u_user = u_user.replace('@', '')

            # 🛡️ [ الرادار يعمل في الكواليس ]
            try:
                user_info = await bot.get_chat(u_id)
                is_female = await deep_privacy_scan(user_info, bot)
            except:
                is_female = False 

            # ⚖️ [ نظام الحماية + الضيافة الموحد ]
            if is_female:
                user_link = f"<b>{raw_name}</b>"
            elif current_chat != u_home:
                # بطل ضيف (شاب)
                if u_user:
                    user_link = f'<a href="https://t.me/{u_user}">{raw_name}</a>'
                else:
                    user_link = f'<a href="tg://user?id={u_id}">{raw_name}</a>'
            else:
                # صاحب البيت (شاب)
                user_link = f"<b>{raw_name}</b>"
            
            msg += f"{medal} ا:ا ⇠ {user_link} + (<code>{pts}</code>ن)\n"
            msg += f"🔹 السرعة: ⏱ <code>{time_val}s</code>\n"
    else:
        msg += "💤 <b>انتهى الوقت دون حسم!</b>\n"
    
    msg += "  ━━━━━━━━━━━━━━━━━━\n"

    # --- [ 2. عرض المخطئين (إجابات خاطئة) ] ---
    if losers:
        msg += "  ╭─── { <b>إجابات خاطئة</b> } ───\n"
        for l in losers:
            raw_l_name = l.get('name', 'لاعب')
            l_id = l.get('id')
            l_user = l.get('user_name')
            l_home = str(l.get('home_cid', ''))
            penalty = l.get('penalty', 5)
            current_chat = str(chat_id)

            if l_user: l_user = l_user.replace('@', '')

            # 🛡️ [ الفحص الخفي للمخطئين ]
            try:
                l_user_info = await bot.get_chat(l_id)
                l_is_female = await deep_privacy_scan(l_user_info, bot)
            except:
                l_is_female = False

            # ⚖️ [ نظام الحماية + الضيافة للمخطئين ]
            if l_is_female:
                l_link = f"<b>{raw_l_name}</b>"
            elif current_chat != l_home:
                # ضيف مخطئ (شاب)
                if l_user:
                    l_link = f'<a href="https://t.me/{l_user}">{raw_l_name}</a>'
                else:
                    l_link = f'<a href="tg://user?id={l_id}">{raw_l_name}</a>'
            else:
                # ابن المجموعة (شاب)
                l_link = f"<b>{raw_l_name}</b>"
            
            msg += f"❌ ا:ا ⇠ {l_link} (خصم <code>{penalty}</code>ن)\n"
        msg += "  ╰──────────────────\n"
        msg += "  ━━━━━━━━━━━━━━━━━━\n"
    
    # --- [ 3. إحصائيات المجموعات (نظام الفرسان المطور) ] ---
    if is_public and group_scores:
        msg += "\n👥 <b>تـنـافـس الـمـجـمـوعـات :</b>\n"
        
        group_ranking = []
        for gid, players in group_scores.items():
            if players:
                total_group_pts = sum(p['points'] for p in players.values())
                sorted_local_players = sorted(players.values(), key=lambda x: x['points'], reverse=True)
                group_ranking.append({'id': gid, 'points': total_group_pts, 'players': sorted_local_players})
        
        sorted_groups = sorted(group_ranking, key=lambda x: x['points'], reverse=True)
        # --- [ 3. إحصائيات المجموعات (نظام الفرسان المطور) ] ---
        for i, g in enumerate(sorted_groups):
            g_id_str = str(g['id'])
            g_name = group_names.get(g_id_str, f"جروب {g_id_str}") if group_names else f"جروب {g_id_str}"
            g_medal = "⭐" if i == 0 else "🔹"
            
            msg += f"\n{g_medal} <b>{g_name}</b>\n"
            msg += f"🔹 رصيد المجموعة: (<code>{g['points']}</code>ن)\n"
            
            # --- [ تعديل الربط الذكي والحماية ] ---
            for player in g['players']:
                # 🔍 محاولة استعادة البيانات المفقودة من القائمة العالمية
                p_id = player.get('id')
                p_name = player.get('name', 'لاعب مجهول')
                p_user = player.get('user_name') 
                p_home = g_id_str # المجموعة الحالية هي موطنه
                current_chat = str(chat_id)

                # إذا لم نجد الـ ID في الـ player، نبحث عنه في الـ winners لربط الرابط
                if not p_id:
                    for w in winners:
                        if w.get('name') == p_name:
                            p_id = w.get('id')
                            p_user = w.get('user_name')
                            break
                
                # تنظيف اليوزر
                if p_user: p_user = p_user.replace('@', '')

                # 🛡️ الرادار الخفي لحماية البنات
                p_is_female = False
                if p_id:
                    try:
                        p_info = await bot.get_chat(p_id)
                        p_is_female = await deep_privacy_scan(p_info, bot)
                    except: pass

                # ⚖️ تطبيق قواعد الحماية + الضيافة (الربط الذكي)
                if p_is_female:
                    p_link = f"<b>{p_name}</b>"
                elif p_id:
                    if current_chat != p_home:
                        # بطل ضيف (تفعيل الرابط)
                        if p_user:
                            p_link = f'<a href="https://t.me/{p_user}">{p_name}</a>'
                        else:
                            p_link = f'<a href="tg://user?id={p_id}">{p_name}</a>'
                    else:
                        # ابن المجموعة (اسم عريض فقط)
                        p_link = f"<b>{p_name}</b>"
                else:
                    # في حال فشل كل شيء، نكتفي بالاسم
                    p_link = f"<b>{p_name}</b>"

                icon = "👤 ا:ا"
                msg += f"{icon} {p_link} ⇠ <b>{player.get('points', 0)}</b> ن\n"
            
            msg += "┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈┈\n"

    msg += "\n🔥 <i>استعد.. السؤال التالي في الطريق!</i>"

    try:
        return await bot.send_message(chat_id, msg, parse_mode="HTML")
    except Exception as e:
        import logging
        logging.error(f"⚠️ HTML Parsing Error: {e}")
        clean_text = msg.replace("<b>", "").replace("</b>", "").replace("<code>", "").replace("</code>", "").replace("<i>", "").replace("</i>", "")
        return await bot.send_message(chat_id, clean_text)
        

async def send_broadcast_final_results(chat_id, scores, total_q, group_names=None):
    try:
        msg = "🌍 <b>تـم اخـتـتـام المسابقة الـعـالـمـيـة</b> 🌍\n"
        msg += ". ━━━━━━━━━━━━━━━━━━\n"
        msg += "🏆 <b>: { كـشـف نـتـائـج الـمـجـموعـات }</b>\n"
        msg += "  ━━━━━━━━━━━━━━━━━━\n\n"

        all_global_players = {}
        group_summary = []
        found_any_score = False

        # --- [ 1. معالجة البيانات ] ---
        for gid, players in scores.items():
            if not players: continue
            
            group_players_list = []
            group_total_pts = 0

            # ترتيب لاعبي المجموعة داخلياً
            sorted_p = sorted(players.items(), key=lambda x: x[1].get('points', 0) if isinstance(x[1], dict) else 0, reverse=True)

            for uid, p_data in sorted_p:
                found_any_score = True
                pts = p_data.get('points', 0) if isinstance(p_data, dict) else 0
                name = p_data.get('name', 'لاعب')
                
                group_total_pts += pts

                # 🛡️ [ تنفيذ الماسح الليزري لحماية البنات ]
                try:
                    # نجلب كائن المستخدم للفحص (اليوزر والبايو)
                    user_info = await bot.get_chat(uid)
                    # الفحص يستخدم "التقشير والتشظي" داخلياً لاتخاذ القرار
                    is_female = await deep_privacy_scan(user_info, bot)
                except:
                    is_female = False # في حال الفشل نعتبره حساب عادي

                # تحديد نوع الرابط (مع الحفاظ على الاسم الأصلي المزخرف)
                if is_female:
                    # اسم البنت يظهر كـ نص غليظ فقط (بدون رابط)
                    p_link = f"<b>{name}</b>"
                else:
                    # اسم الشاب يظهر كرابط قابل للضغط
                    p_link = f'<a href="tg://user?id={uid}">{name}</a>'
                
                # إضافة اللاعب للقائمة
                group_players_list.append(f"👤 : {p_link} [ <b>{pts}</b> ن ]")

                u_id_str = str(uid)
                if u_id_str not in all_global_players:
                    all_global_players[u_id_str] = {"name": name, "points": 0}
                all_global_players[u_id_str]['points'] += pts

            g_name = group_names.get(str(gid), f"جروب {gid}") if group_names else f"جروب {gid}"
            group_summary.append({
                'name': g_name,
                'total': group_total_pts,
                'players_text': "\n".join(group_players_list)
            })

        # --- [ 2. عرض المجموعات مع ترتيب الرموز ] ---  
        if group_summary:
            sorted_groups = sorted(group_summary, key=lambda x: x['total'], reverse=True)
            
            for i, g in enumerate(sorted_groups, 1):
                is_winner = (i == 1)
                medal = "🥇 :" if i == 1 else "🥈 :" if i == 2 else "🥉 :" if i == 3 else "🔹 :"
                
                win_status = " ✨ [+1 🏆 : فوز]" if is_winner else ""
                
                msg += f"{medal} <b>{g['name']}</b> {win_status}\n"
                msg += f"📊 : إجمالي النقاط ( <code>{g['total']}</code> ن )\n"
                msg += f"{g['players_text']}\n"
                msg += "  ┅┅┅┅┅┅┅┅┅┅┅┅┅┅┅┅\n"
 
        # --- [ 3. ملوك الإذاعة (الترتيب الفردي) ] ---

        if all_global_players:
            msg += "\n👑 <b>: تـرتـيـب مـلـوك الـعـالـم :</b>\n"
            # ترتيب اللاعبين حسب النقاط
            sorted_global = sorted(all_global_players.items(), key=lambda x: x[1]['points'], reverse=True)
            
            for i, (uid, p) in enumerate(sorted_global[:5], 1):
                icon = "🥇 :" if i == 1 else "🥈 :" if i == 2 else "🥉 :" if i == 3 else "👤 :"
                
                # الحد الأدنى للذكاء يبدأ من 30% لزيادة حدة المنافسة
                max_possible_pts = total_q * 110
                if max_possible_pts > 0:
                    # نسبة الأداء (السرعة + الدقة) مضروبة في 70 لضبط المتبقي
                    performance_ratio = (p['points'] / max_possible_pts) * 70
                    # النتيجة تبدأ من 30 وتصل لـ 100 كحد أقصى
                    iq = min(int(performance_ratio) + 30, 100)
                else:
                    iq = 30

                # 🛡️ [ فحص الخصوصية لملوك العالم ]
                try:
                    user_info = await bot.get_chat(int(uid))
                    is_female = await deep_privacy_scan(user_info, bot)
                except:
                    is_female = False

                # حماية الرابط مع الحفاظ على الاسم الأصلي المزخرف
                if is_female:
                    p_link = f"<b>{p['name']}</b>"
                else:
                    p_link = f'<a href="tg://user?id={uid}">{p["name"]}</a>'

                msg += f"{icon} {p_link} ⇠ <b>{p['points']}</b> ن (🧠 {iq}% IQ)\n"

        if not found_any_score:
            msg = "🌍 <b>: انتهت الإذاعة !</b>\n\nلم يتم تسجيل أي نقاط ."
        else:
            # --- [ 4. إحصائيات المشاركة والختام ] ---
            total_participants = len(all_global_players)
            msg += "\n  ━━━━━━━━━━━━━━━━━━\n"
            msg += f"👥 : عدد المشاركين ( <b>{total_participants}</b> بطل )\n"
            msg += f"📋 : إجمالي الأسئلة ( <b>{total_q}</b> سؤال )\n"
            msg += "✅ : تم تسجيل بيانات المسابقة في ملف كل لاعب  !"

        return await bot.send_message(chat_id, msg, parse_mode="HTML")

    except Exception as e:
        import logging
        logging.error(f"❌ : خطأ في الإذاعة : {e}")
        
# ==========================================
# ==========================================
async def send_creative_results2(chat_id, correct_ans, winners, overall_scores):
    """تصميم ياسر المطور: قالب الصناديق الملكية الموحد"""
    
    # 1. صندوق الإجابة الصحيحة (بنفس نمط القالب الذي طلبته)
    msg =  "<b>⚡️〔 نـهـايـة الـجـولـة 〕⚡️</b>\n"
    msg += "<b>┌─────────────────╼</b>\n"
    msg += f"<b>│ ✅ الإجـابـة:</b> <code>{correct_ans}</code>\n"
    msg += "<b>└─────────────────╼</b>\n\n"
    
    # 2. صندوق المتفوقين في السؤال الحالي
    if winners:
        msg += "<b>🌟〔 الـمـتـفـوقـون 〕🌟</b>\n"
        msg += "<b>┌─────────────────╼</b>\n"
        for i, w in enumerate(winners, 1):
            msg += f"<b>│ {i} ↤</b> {w['name']} <b>+1 🎖</b>\n"
        msg += "<b>└─────────────────╼</b>\n\n"
    else:
        msg += "<b>🛑 ↢ لم ينجح أحد في هذا التحدي!</b>\n"
        msg += "<b>────────────────────</b>\n\n"
    
    # 3. صندوق لوحة الصدارة العامة (نفس القالب الموحد)
    leaderboard = sorted(overall_scores.values(), key=lambda x: x['points'], reverse=True)
    msg += "<b>🏆〔 لـوحـة الـصـدارة الـعـامـة 〕🏆</b>\n"
    msg += "<b>┌─────────────────╼</b>\n"
    
    medals = ["🥇", "🥈", "🥉"]
    for i, player in enumerate(leaderboard[:5]): 
        icon = medals[i] if i < 3 else "👤"
        # جعل الاسم والنقاط داخل نفس نمط الصندوق
        msg += f"<b>│ {icon} ↤</b> {player['name']} <b>— ⦗ {player['points']} ⦘</b>\n"
            
    msg += "<b>└─────────────────╼</b>\n"
    msg += "<b>✨ تـابـع الـتـألق.. فـالـقـادم أجـمـل! ✨</b>"

    # --- [ الإرسال والمعالجة ] ---
    try:
        return await bot.send_message(chat_id, msg, parse_mode="HTML")
    except Exception as e:
        import logging
        logging.error(f"⚠️ HTML Error: {e}")
        import re
        clean_msg = re.sub('<[^<]+?>', '', msg)
        return await bot.send_message(chat_id, clean_msg)
        

async def send_final_results2(chat_id, overall_scores, total_q):
    """
    🥇 تصميم ياسر الملكي - نسخة المسابقات الخاصة V3
    ضبط المحاذاة اليمينية باستخدام الفواصل النقطية :
    """
    try:
        # 🎨 رأس القالب
        msg =  "  ━━━━━━━━━━━━━━━━━━━\n"
        msg += "🏁 <b>: انـتـهـت الـمـسـابـقـة الـخـاصـة</b>\n"
        msg += "🔥 <b>: حـصـاد الـعـمـالـقـة والأبـطـال</b>\n"
        msg += "  ━━━━━━━━━━━━━━━━━━━\n\n"
        
        msg += "🏆 <b>: { لـوحـة الـشـرف والـتـتـويـج }</b>\n\n"

        # ترتيب اللاعبين حسب النقاط
        sorted_players = sorted(overall_scores.values(), key=lambda x: x['points'], reverse=True)
        max_possible_pts = total_q * 10 
        
        # الأيقونات مع الفواصل لضبط اليمين
        medals = ["🥇 :", "🥈 :", "🥉 :", "👤 :", "👤 :"]
        for i, player in enumerate(sorted_players[:10]):  # عرض توب 10
            # اختيار الأيقونة المناسبة
            icon = medals[i] if i < 5 else "👤 :"
            
            # 🧠 [ حساب IQ الجولة المسبوك - يبدأ من 30% ]
            # أقصى نقاط ممكنة (110 ن × عدد الأسئلة) بناءً على نظام السرعة
            max_possible_pts = total_q * 110 
            if max_possible_pts > 0:
                # نسبة الأداء مضروبة في 70 لضبط الميزان مع البداية من 30
                performance_ratio = (player.get('points', 0) / max_possible_pts) * 70
                round_iq = min(int(performance_ratio) + 30, 100)
            else:
                round_iq = 30
            
            # جلب الاسم بأمان لمنع أي خطأ KeyError
            p_name = player.get('name', 'لاعب مجهول')
            
            # السطر الذهبي (بدون منشن - اسم فقط عريض)
            msg += f"{icon} <b>{p_name}</b>\n"
            msg += f"🏅 <b>:</b> المركز ( {i+1} ) ⇠ <b>{player.get('points', 0)}</b> ن\n"
            msg += f"🧠 <b>:</b> ذكاء الجولة ⇠ <code>{round_iq}% IQ</code>\n"
            
            # تمييز بطل المسابقة الخاصة
            if i == 0:
                msg += "✨ <b>: [+1 🔥 فـوز خـاص مـسـجـل]</b>\n"
                
            msg += "  ┅┅┅┅┅┅┅┅┅┅┅┅┅┅┅┅\n"

        # 📊 ذيل القالب المحسن
        msg += "\n📊 <b>: إحـصـائـيـات الـتـفـاعـل</b>\n"
        msg += f"📋 <b>:</b> إجمالي الأسئلة ⇠ ( <b>{total_q}</b> )\n"
        # تأكد من استخدام اسم المتغير الصحيح لإجمالي اللاعبين لديك (مثلاً overall_scores)
        total_players = len(overall_scores) if 'overall_scores' in locals() else len(sorted_players)
        msg += f"👥 <b>:</b> عدد المشاركين ⇠ ( <b>{total_players}</b> )\n"
        msg += "  ━━━━━━━━━━━━━━━━━━━\n"
        msg += "❤️ <b>: تهانينا للفائزين وحظاً أوفر للبقية</b>\n"
        msg += "✅ <b>: تم ترحيل الألقاب والجوائز بنجاح</b>"

        await bot.send_message(chat_id, msg, parse_mode="HTML")
        
    except Exception as e:
        import logging
        logging.error(f"❌ خطأ في العملية السابقة: {e}")
        
# ============================================================
# 1. دوال النظام الذكي (الرتب، التخصصات، الحسابات)
# ============================================================

def generate_14_digit_bank():
    """توليد رقم حساب بنكي احترافي مكون من 14 رقم"""
    return "".join([str(random.randint(0, 9)) for _ in range(14)])

async def sync_points_to_global_db(group_scores=None, winners_list=None, cat_name="عام", is_special=False, quiz_id=None):
    """
    👑 محرك المزامنة المطور - نسخة Questions Bot 2026
    - يعتمد على حالة is_correct لحساب عدد الإجابات بدلاً من النقاط.
    - يدعم نظام النقاط المتغير (السرعة + الـ 100 نقطة الأساسية).
    """
    try:
        # 1️⃣ جلب البيانات من سجل الإجابات إذا توفر quiz_id
        if quiz_id:
            try:
                res_log = supabase.table("answers_log").select("*").eq("quiz_id", quiz_id).execute()
                if res_log.data:
                    group_scores = {}
                    for row in res_log.data:
                        cid = row['chat_id']
                        uid = str(row['user_id'])
                        
                        if cid not in group_scores: group_scores[cid] = {}
                        if uid not in group_scores[cid]:
                            group_scores[cid][uid] = {
                                'name': row['user_name'], 
                                'points': 0, 
                                'correct_count': 0  # ✅ عداد جديد للإجابات الصحيحة
                            }
                        
                        # إضافة النقاط (سواء كانت 100 أو نقاط السرعة)
                        group_scores[cid][uid]['points'] += row.get('points_earned', 0)
                        
                        # ✅ التعديل الجوهري: العد بناءً على حالة الإجابة وليس قيمة النقاط
                        if row.get('is_correct') is True:
                            group_scores[cid][uid]['correct_count'] += 1
                            
                    logging.info(f"📊 تم جلب حصاد {len(res_log.data)} إجابة. تم فصل النقاط عن عدد الإجابات بنجاح.")
            except Exception as e:
                logging.error(f"❌ خطأ في سحب بيانات اللوج: {e}")

        if not group_scores:
            logging.warning("⚠️ لا توجد بيانات للترحيل.")
            return

        # 2️⃣ تحديد المجموعات الفائزة
        winning_groups = winners_list if winners_list else []
        if not winning_groups:
            group_totals = {gid: sum(p.get('points', 0) for p in players.values()) 
                            for gid, players in group_scores.items()}
            if group_totals:
                top_group_id = max(group_totals, key=group_totals.get)
                winning_groups = [top_group_id]

        # 3️⃣ تجميع حصاد اللاعبين النهائي
        final_tallies = {}
        for cid, players in group_scores.items():
            is_the_champion_group = (cid in winning_groups)
            for uid, p_data in players.items():
                u_id = int(uid)
                if u_id not in final_tallies:
                    final_tallies[u_id] = {
                        "name": p_data.get('name', 'لاعب مجهول'), 
                        "pts": 0, 
                        "ans_count": 0, 
                        "won_round": 0
                    }
                
                final_tallies[u_id]["pts"] += p_data.get('points', 0)
                # ✅ هنا نستخدم العداد الصافي للإجابات الصحيحة الذي جمعناه من اللوج
                final_tallies[u_id]["ans_count"] += p_data.get('correct_count', 0)
                
                if is_the_champion_group:
                    final_tallies[u_id]["won_round"] = 1

        # 4️⃣ المزامنة مع الجدول الجديد users_global_profile
        for uid, data in final_tallies.items():
            try:
                res = supabase.table("users_global_profile").select("*").eq("user_id", uid).execute()
                
                def calculate_rank(total_ans):
                    if total_ans <= 50: return "🌱 عضو جديد"
                    elif total_ans <= 150: return "📚 طالب مجتهد"
                    elif total_ans <= 300: return "🎓 خريج متميز"
                    elif total_ans <= 600: return "📑 باحث علمي"
                    elif total_ans <= 1200: return "🔬 عالم فذ"
                    elif total_ans <= 2500: return "🏛️ بروفيسور"
                    elif total_ans <= 5000: return "👑 أسطورة زدني"
                    else: return "✨ سلطان المعرفة"

                def calculate_specialty(stats):
                    if not stats: return "هاوي"
                    top_cat = max(stats, key=stats.get)
                    score = stats[top_cat]
                    if score > 1000: return f"🏅 أسطورة {top_cat}"
                    elif score > 500: return f"👨‍🔬 عالم {top_cat}"
                    elif score > 100: return f"📜 خبير {top_cat}"
                    else: return f"🔍 محب لـ {top_cat}"

                if res.data:
                    current = res.data[0]
                    current_stats = current.get('category_stats') or {}
                    # تحديث إحصائيات القسم بناءً على عدد الإجابات الصحيحة الصافي
                    current_stats[cat_name] = current_stats.get(cat_name, 0) + data['ans_count']
                    
                    total_ans = (current.get('correct_answers_count') or 0) + data['ans_count']
                    titles = current.get('titles', [])
                    
                    if is_special and data['won_round'] > 0:
                        if "🔥 : نجم المسابقات" not in titles:
                            titles.append("🔥 : نجم المسابقات")

                    upd_payload = {
                        "user_name": data['name'],
                        "total_points": (current.get('total_points') or 0) + data['pts'],
                        "wallet": (current.get('wallet') or 0) + data['pts'],
                        "correct_answers_count": total_ans, # ✅ عدد الإجابات الحقيقي
                        "iq_score": min(150, (current.get('iq_score') or 50) + (data['ans_count'] // 5)),
                        "educational_rank": calculate_rank(total_ans),
                        "category_stats": current_stats,
                        "specialty_title": calculate_specialty(current_stats),
                        "titles": titles,
                        "last_update": "now()"
                    }

                    if is_special:
                        upd_payload["special_wins"] = (current.get('special_wins') or 0) + data['won_round']
                    else:
                        upd_payload["total_wins"] = (current.get('total_wins') or 0) + data['won_round']
                    
                    supabase.table("users_global_profile").update(upd_payload).eq("user_id", uid).execute()
                else:
                    # إنشاء مستخدم جديد تماماً
                    new_payload = {
                        "user_id": uid, 
                        "user_name": data['name'],
                        "bank_account": generate_14_digit_bank(), # تم استدعاء الدالة المكونة من 14 رقم
                        "total_points": data['pts'], 
                        "wallet": data['pts'],
                        "correct_answers_count": data['ans_count'],
                        "total_wins": data['won_round'] if not is_special else 0,
                        "special_wins": data['won_round'] if is_special else 0,
                        "iq_score": 60,
                        "category_stats": {cat_name: data['ans_count']},
                        "educational_rank": calculate_rank(data['ans_count']),
                        "specialty_title": calculate_specialty({cat_name: data['ans_count']}),
                        "titles": ["🌱 : عضو جديد"], 
                        "inventory": [],
                        "cards_inventory": {"time": 1, "full": 1, "shield": 1, "reveal": 1, "double": 1, "letter": 1}
                    }
                    supabase.table("users_global_profile").insert(new_payload).execute()
                
                logging.info(f"✅ تم ترحيل بيانات البطل: {data['name']} بنجاح.")
            except Exception as e:
                logging.error(f"❌ خطأ في تحديث بروفايل {uid}: {e}")

    except Exception as fatal_e:
        logging.error(f"🚨 خطأ قاتل في محرك المزامنة: {fatal_e}")
            
# ============================================================
# دالة تحديث سجلات المجموعة (الذكاء الجماعي)
# الوظيفة: حفظ نقاط المجموعة وتحديد العضو الأبرز فيها
# ============================================================
async def update_group_stats(group_id: int, group_name: str, user_id: int, user_name: str, points: int):
    try:
        # 1. محاولة جلب بيانات المجموعة
        res = supabase.table("groups_global_stats").select("*").eq("group_id", group_id).execute()
        
        if not res.data:
            # إذا كانت المجموعة جديدة، ننشئ لها سجلاً
            supabase.table("groups_global_stats").insert({
                "group_id": group_id,
                "group_name": group_name,
                "total_points": points,
                "top_member_name": user_name,
                "top_member_id": user_id,
                "members_count": 1
            }).execute()
        else:
            group = res.data[0]
            new_total_points = group['total_points'] + points
            
            # 2. منطق تحديد "الأكثر ذكاءً": (يمكنك ربطها بنقاط العضو الحالية)
            # هنا سنفترض أن العضو الذي جلب نقاطاً للمجموعة الآن يُقارن بالعضو المسجل سابقاً
            # ملاحظة: يمكنك تطويرها لتقارن مع الـ IQ_score من جدول المستخدمين
            
            update_data = {
                "total_points": new_total_points,
                "group_name": group_name, # لتحديث الاسم إذا تغير
                "last_activity": "now()"
            }
            
            # تحديث العضو المتصدر إذا حقق نقاطاً عالية (مثال)
            if points > 50: # إذا أضاف أكثر من 50 نقطة في مرة واحدة
                update_data["top_member_name"] = user_name
                update_data["top_member_id"] = user_id

            supabase.table("groups_global_stats").update(update_data).eq("group_id", group_id).execute()

    except Exception as e:
        print(f"Error updating group stats: {e}")
                    
# --- إصلاح اتجاه النصوص (حلك الذكي) ---
def fix_arabic(text):
    return "\u200F" + str(text) if text else ""

def fix_number(text):
    return "\u200E" + str(text) if text else ""

# --- دالة جلب صورة البروفايل ومعالجتها (الحل السريع لـ Aiogram) ---
async def get_profile_img(bot, user_id):
    try:
        photos = await bot.get_user_profile_photos(user_id, limit=1)
        if photos.total_count > 0:
            file_id = photos.photos[0][-1].file_id
            file = await bot.get_file(file_id)
            
            photo_bytes = io.BytesIO()
            await bot.download_file(file.file_path, destination=photo_bytes)
            photo_bytes.seek(0)

            p_raw = Image.open(photo_bytes).convert("RGBA")
            size = (220, 220)
            p_raw = p_raw.resize(size, Image.LANCZOS)
            
            mask = Image.new("L", size, 0)
            ImageDraw.Draw(mask).ellipse((0, 0) + size, fill=255)
            
            output = Image.new("RGBA", size, (0, 0, 0, 0))
            output.paste(p_raw, (0, 0), mask)
            return output
        return None
    except Exception as e:
        logging.warning(f"⚠️ فشل جلب صورة المستخدم {user_id}: {e}")
        return None

# --- الدالة الرئيسية لتوليد البطاقة (نسخة الأعلام والهوية الموحدة) ---
async def generate_zidni_card(user_id: int, bot, supabase):
    base_path = "assets/fonts/"
    paths = {
        "font": os.path.join(base_path, "font.ttf"),
        "emoji": os.path.join(base_path, "emoji.ttf"),
        "card": "assets/images/zidni_card.png"
    }

    try:
        # 1. جلب البيانات من Supabase
        res = supabase.table("users_global_profile").select("*").eq("user_id", int(user_id)).execute()
        if not res.data:
            return None, None
        user_db = res.data[0]

        # 2. فتح القالب والخطوط
        template = Image.open(paths["card"]).convert("RGBA")
        font_main = ImageFont.truetype(paths["font"], 35)
        font_info = ImageFont.truetype(paths["font"], 30)

        # 3. جلب ووضع صورة البروفايل
        profile_circle = await get_profile_img(bot, user_id)
        if profile_circle:
            template.paste(profile_circle, (83, 62), profile_circle)

        # 4. تجهيز البيانات النصية (تم حذف specialty_title وإضافة flag)
        name = str(user_db.get("user_name", "غير معروف"))[:20]
        # الرتبة الآن تعرض المستوى التعليمي فقط
        rank = f"{user_db.get('educational_rank', 'طالب')}"
        wallet = user_db.get("wallet", 0)
        acc_num = user_db.get("bank_account", "0000")
        # جلب العلم من القاعدة (الافتراضي علم اليمن إذا لم يوجد)
        flag = user_db.get("country_flag", "")

        # 5. الرسم على البطاقة (بإحداثياتك المعتمدة)
        with Pilmoji(template) as pilmoji:
            white, gold = (255, 255, 255), (212, 175, 55)
            # نمرر مسار خط الإيموجي لضمان ظهور علم اليمن والرموز
            emoji_path = paths["emoji"] if os.path.exists(paths["emoji"]) else None

            # الاسم
            pilmoji.text((795, 210), fix_arabic(name), font=font_main, fill=white, anchor="ra", emoji_fontpath=emoji_path)
            # الدولة
            pilmoji.text((795, 280), fix_arabic("اليمن"), font=font_info, fill=gold, anchor="ra", emoji_fontpath=emoji_path)
            # الرتبة
            pilmoji.text((795, 345), fix_arabic(rank), font=font_info, fill=white, anchor="ra", emoji_fontpath=emoji_path)
            # الرصيد
            pilmoji.text((795, 415), fix_arabic(f"{wallet:,} ن"), font=font_info, fill=gold, anchor="ra", emoji_fontpath=emoji_path)
            # رقم الحساب
            pilmoji.text((505, 580), fix_number(f"ZD-{acc_num}"), font=font_info, fill=white, anchor="mm")
        # 6. إخراج الصورة والبيانات (للكابشن)
        output = io.BytesIO()
        template.save(output, format="PNG")
        output.seek(0)
        
        return output, user_db

    except Exception as e:
        logging.error(f"❌ خطأ في generate_zidni_card: {e}")
        return None, None
        
# ============================================================
# دالة تنسيق بطاقة المجموعة (Top Groups)
# ============================================================
def format_group_card(group_data: dict):
    g = group_data
    card = f"<b>🏰 : إحـصـائـيـات الـمـجـمـوعـة 🏰</b>\n"
    card += "<b>— — — — — — — — — — — —</b>\n"
    card += f"👥 <b>: المجموعة ⇠</b> <code>{g.get('group_name')}</code>\n"
    card += f"💰 <b>: رصيد النقاط ⇠</b> <code>{g.get('total_points', 0)}</code> ن\n"
    card += f"📊 <b>: عدد المشتركين ⇠</b> <code>{g.get('members_count', 0)}</code>\n"
    card += "<b>— — — — — — — — — — — —</b>\n"
    card += f"🧠 <b>: العضو الأبرز ⇠</b> {g.get('top_member_name', 'لا يوجد')}\n"
    card += f"🏆 <b>: مـركـز الـمـجـمـوعـة ⇠</b> [ عـالـمـي ]\n"
    card += "<b>— — — — — — — — — — — —</b>\n"
    card += "✨ <i>تعاونوا لتصبح مجموعتكم هي الأكثر ذكاءً!</i>"
    
    return card
def update_system_setting(setting_name, new_value):
    """
    تحديث إعدادات النظام في جدول system_settings
    """
    try:
        # نقوم بتحديث القيمة حيث اسم الإعداد هو ACTIVE_GROQ_KEY
        res = supabase.table("system_settings").update({"key_value": new_value}).eq("key_name", setting_name).execute()
        
        # إذا تمت العملية بنجاح نرجع True
        if res.data:
            return True
        return False
    except Exception as e:
        logging.error(f"Error updating system setting: {e}")
        return False
# ==========================================
# 1. كيبوردات التحكم الرئيسية (Main Keyboards)
# ==========================================
def get_main_control_kb(user_id):
    """توليد كيبورد لوحة التحكم الرئيسية مشفرة بآيدي المستخدم"""
    kb = InlineKeyboardMarkup(row_width=2).add(
        InlineKeyboardButton("📝 إضافة خاصة", callback_data=f"custom_add_{user_id}"),
        InlineKeyboardButton("📅 جلسة سابقة", callback_data=f"dev_session_{user_id}"),
        InlineKeyboardButton("🛒 المتجر العالمي", callback_data=f"open_shop_{user_id}"),
        InlineKeyboardButton("🏆 تجهيز مسابقة", callback_data=f"setup_quiz_{user_id}"),
        InlineKeyboardButton("📊 لوحة الصدارة", callback_data=f"dev_leaderboard_{user_id}"),
        InlineKeyboardButton("🛑 إغلاق", callback_data=f"close_bot_{user_id}")
    )
    return kb


# 3️⃣ [ دالة عرض القائمة الرئيسية للأقسام ]
async def custom_add_menu(c, owner_id, state):
    if state:
        await state.finish()
        
    kb = get_categories_kb(owner_id) 
    await c.message.edit_text(
        "⚙️ **لوحة إعدادات أقسامك الخاصة:**\n\nأهلاً بك! اختر من الخيارات أدناه لإدارة بنك أسئلتك وإضافة أقسام جديدة:",
        reply_markup=kb,
        parse_mode="Markdown"
    )
    await c.answer()
# ==========================================
# ---الدالة التي طلبتها (تأكد أنها موجودة بهذا الاسم) ---
# ==========================================
def get_categories_kb(user_id):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("➕ إضافة قسم جديد", callback_data=f"add_new_cat_{user_id}"))
    kb.add(InlineKeyboardButton("📋 قائمة الأقسام", callback_data=f"list_cats_{user_id}"))
    kb.add(InlineKeyboardButton("🔙 الرجوع لصفحة التحكم", callback_data=f"back_to_main_{user_id}"))
    
    return kb

# ==========================================
# 2. دوال عرض الواجهات الموحدة (UI Controllers)
# ==========================================
async def show_category_settings_ui(message: types.Message, cat_id, owner_id, is_edit=True):
    """الدالة الموحدة لعرض إعدادات القسم بضغطة واحدة"""
    # جلب البيانات من سوبابيس
    cat_res = supabase.table("categories").select("name").eq("id", cat_id).single().execute()
    q_res = supabase.table("questions").select("*", count="exact").eq("category_id", cat_id).execute()
    
    cat_name = cat_res.data['name']
    q_count = q_res.count if q_res.count else 0
    
    txt = (f"⚙️ إعدادات القسم: {cat_name}\n\n"
           f"📊 عدد الأسئلة المضافة: {q_count}\n"
           f"ماذا تريد أن تفعل الآن؟")

    # بناء الأزرار وتشفيرها بالآيدي المزدوج (cat_id + owner_id)
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("➕ إضافة سؤال", callback_data=f"add_q_{cat_id}_{owner_id}"),
        InlineKeyboardButton("📝 تعديل الاسم", callback_data=f"edit_cat_{cat_id}_{owner_id}")
    )
    kb.add(
        InlineKeyboardButton("🔍 عرض الأسئلة", callback_data=f"view_qs_{cat_id}_{owner_id}"),
        InlineKeyboardButton("🗑️ حذف الأسئلة", callback_data=f"del_qs_menu_{cat_id}_{owner_id}")
    )
    kb.add(InlineKeyboardButton("❌ حذف القسم", callback_data=f"confirm_del_cat_{cat_id}_{owner_id}"))
    kb.add(
        InlineKeyboardButton("🔙 رجوع", callback_data=f"list_cats_{owner_id}"),
        InlineKeyboardButton("🏠 الرئيسية", callback_data=f"back_to_control_{owner_id}")
    )
    
    if is_edit:
        await message.edit_text(txt, reply_markup=kb, parse_mode="Markdown")
    else:
        # تستخدم هذه بعد الـ message_handler (save_cat) لأن الرسالة السابقة قد حذفت
        await message.answer(txt, reply_markup=kb, parse_mode="Markdown")
# ==========================================
# ==========================================
def get_setup_quiz_kb(user_id):
    """كيبورد تهيئة المسابقة مشفر بآيدي المستخدم"""
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("👥 أقسام الأعضاء (إسئلة الاعضاء)", callback_data=f"members_setup_step1_{user_id}"),
        InlineKeyboardButton("👤 أقسامك الخاصة (مكتبتي)", callback_data=f"my_setup_step1_{user_id}"),
        InlineKeyboardButton("🤖 أقسام البوت (الرسمية)", callback_data=f"bot_setup_step1_{user_id}"),
        InlineKeyboardButton("🔙 رجوع للقائمة الرئيسية", callback_data=f"back_to_control_{user_id}")
    )
    return kb

# ============================================================
# 1. دوال الأزرار (Keyboards)
# ============================================================
def get_leaderboard_keyboard():
    """لوحة التحكم الرئيسية"""
    keyboard = InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        InlineKeyboardButton("💰 : قـائـمـة أغـنـيـاء الـعـرب", callback_data="top_wealth"),
        InlineKeyboardButton("🧠 : سـجـل أذكـيـاء الـمـجـرات", callback_data="top_iq"),
        InlineKeyboardButton("🏰 : تـرتـيـب أقـوى الـمـجـمـوعـات", callback_data="top_groups"),
        InlineKeyboardButton("❌ : إغـلاق الـسـجـل", callback_data="close_card")
    )
    return keyboard

def get_back_keyboard():
    """أزرار الرجوع والإغلاق (تظهر أسفل كل القوالب بلا استثناء)"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
        InlineKeyboardButton("🔙 : رجوع", callback_data="back_to_leaderboard"),
        InlineKeyboardButton("❌ : إغلاق", callback_data="close_card")
    )
    return keyboard

# ============================================================
# 2. رسالة الترحيب الرئيسية
# ============================================================
def get_leaderboard_main_message():
    # 1. القالب البصري للرسالة
    text = (
        "<b>🏆 | مـنـصـة الـشـرف الـعـالـمـيـة</b>\n"
        "<b>— — — — — — — — — — — — — —</b>\n\n"
        "مرحباً بك في سجلات <b>بنك زدني</b> الخالدة، "
        "هنا تُكتب أسماء العمالقة الذين تربعوا على العروش بالذكاء والمال والتحالفات.\n\n"
        "<b>📌 | اختر القائمة التي تود استكشافها:</b>\n\n"
        "💰 <b>: الأغنياء ⇠</b> لعرض أصحاب المليارات.\n"
        "🧠 <b>: الأذكياء ⇠</b> لعرض دهاه العقل وعباقرة الـ IQ.\n"
        "🏰 <b>: المجموعات ⇠</b> لعرض أقوى التحالفات الجماعية.\n\n"
        "<b>— — — — — — — — — — — — — —</b>\n"
        "✨ <i>المنافسة مشتعلة.. هل اسمك موجود بينهم؟</i>"
    )
    return text, get_leaderboard_keyboard()
# ============================================================
# 3. قوالب التنسيق الفخمة (Templates)
# ============================================================

def format_top_iq_list(data: list):
    header = "<b>🧠 : سـجـل أذكـيـاء الـمـجـرات</b>\n"
    header += "<b>— — — — — — — — — — — —</b>\n\n"
    body = ""
    medals = ["🥇 :", "🥈 :", "🥉 :", "🏅 :", "🏅 :", "🏅 :", "🏅 :", "🏅 :", "🏅 :", "🏅 :"]
    for i, user in enumerate(data):
        rank_icon = medals[i]
        name = user.get('user_name', 'مجهول')
        iq = user.get('iq_score', 0); ans = user.get('correct_answers_count', 0)
        rank_n = user.get('educational_rank', 'طالب'); flag = user.get('country_flag', '🌐')
        body += f"{rank_icon} <b>{name}</b> {flag}\n"
        body += f"    🔸 <b>: الرتبة :</b> <code>{rank_n}</code>\n"
        body += f"    🔹 <b>: الذكاء :</b> <code>{iq} IQ</code> | <b>الاجابات :</b> <code>{ans}</code>\n\n"
    return header + body + "<b>— — — — — — — — — — — —</b>"

def format_top_wealth_list(data: list):
    header = "<b>💰 : قـائـمـة أغـنـيـاء الـعـرب</b>\n"
    header += "<b>— — — — — — — — — — — —</b>\n\n"
    body = ""
    medals = ["👑 :", "💎 :", "💰 :", "💵 :", "💵 :", "💵 :", "💵 :", "💵 :", "💵 :", "💵 :"]
    for i, user in enumerate(data):
        rank_icon = medals[i]
        name = user.get('user_name', 'مجهول'); money = user.get('wallet', 0)
        inv = user.get('inventory', []); items = len(inv) if isinstance(inv, list) else 0
        gifts = user.get('special_wins', 0); flag = user.get('country_flag', '🌐')
        body += f"{rank_icon} <b>{name}</b> {flag}\n"
        body += f"    🔸 <b>: الرصيد :</b> <code>{money:,}</code> ن\n"
        body += f"    🔹 <b>: المقتنيات :</b> <code>{items}</code> | <b>الهدايا :</b> <code>{gifts}</code>\n\n"
    return header + body + "<b>— — — — — — — — — — — —</b>"

def format_top_groups_list(data: list):
    header = "<b>🏰 : تـرتـيـب أقـوى الـمـجـمـوعـات</b>\n"
    header += "<b>— — — — — — — — — — — —</b>\n\n"
    body = ""
    medals = ["🏛️ :", "🏟️ :", "🏤 :", "🏰 :", "🏰 :", "🏰 :", "🏰 :", "🏰 :", "🏰 :", "🏰 :"]
    for i, group in enumerate(data):
        rank_icon = medals[i]; g_name = group.get('group_name', 'مجموعة مجهولة')
        pts = group.get('total_points', 0); top_m = group.get('top_member_name', 'لا يوجد')
        m_count = group.get('members_count', 0)
        body += f"{rank_icon} <b>{g_name}</b>\n"
        body += f"    🔸 <b>: إجمالي النقاط :</b> <code>{pts:,}</code> ن\n"
        body += f"    🔹 <b>: العضو الأبرز :</b> <code>{top_m}</code>\n"
        body += f"    👥 <b>: عدد الأعضاء :</b> <code>{m_count}</code> عضواً\n\n"
    return header + body + "<b>— — — — — — — — — — — —</b>"

# ============================================================
# 4. معالج العمليات (Callback Handler)
# ============================================================

@dp.callback_query_handler(lambda c: c.data in ['top_wealth', 'top_iq', 'top_groups', 'back_to_leaderboard', 'close_card'])
async def process_board_navigation(c: types.CallbackQuery):
    action = c.data

    if action == "close_card":
        try: await c.message.delete()
        except: pass
        return await c.answer("تم إغلاق السجل")

    if action == "back_to_leaderboard":
        text, kb = get_leaderboard_main_message()
        return await c.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

    await c.answer("⏳ جاري جلب البيانات...")

    try:
        if action == "top_wealth":
            res = supabase.table("users_global_profile").select("*").order("wallet", desc=True).limit(10).execute()
            text = format_top_wealth_list(res.data)
        elif action == "top_iq":
            res = supabase.table("users_global_profile").select("*").order("iq_score", desc=True).limit(10).execute()
            text = format_top_iq_list(res.data)
        elif action == "top_groups":
            res = supabase.table("groups_global_stats").select("*").order("total_points", desc=True).limit(10).execute()
            text = format_top_groups_list(res.data)

        # تحديث الرسالة مع ضمان وجود أزرار الرجوع والإغلاق في كل القوالب
        await c.message.edit_text(text, reply_markup=get_back_keyboard(), parse_mode="HTML")

    except Exception as e:
        print(f"Leaderboard Error: {e}")
        await c.answer("❌ فشل الاتصال بقاعدة البيانات.", show_alert=True)

# ============================================================
# 5. استدعاء الكلمات والحذف التلقائي (النسخة الصاروخية)
# ============================================================

@dp.message_handler(lambda message: message.text in ['توب', 'التوب', 'الترتيب', 'لوحة الصدارة'] or message.text.startswith('/top'))
async def cmd_show_leaderboard(message: types.Message):
    """استدعاء اللوحة مع ميزة الحذف التلقائي بعد 60 ثانية"""
    text, reply_markup = get_leaderboard_main_message()
    
    # إرسال الرسالة وحفظ الكائن الخاص بها
    sent_msg = await message.reply(text=text, reply_markup=reply_markup, parse_mode="HTML")
    
    # مهمة الحذف التلقائي بعد 60 ثانية
    await asyncio.sleep(60)
    try:
        await sent_msg.delete()
        # اختياري: حذف رسالة المستخدم أيضاً ليبقى الشات نظيفاً
        await message.delete()
    except:
        pass # الرسالة قد تم حذفها يدوياً بالفعل

# ==========================================
# الدوال المساعدة المحدثة (حماية + أسماء حقيقية)
# ==========================================
async def render_members_list(message, eligible_list, selected_list, owner_id):
    """
    eligible_list: قائمة تحتوي على ديكشنري [{id: ..., name: ...}]
    """
    kb = InlineKeyboardMarkup(row_width=2)
    for member in eligible_list:
        m_id = str(member['id'])
        # نستخدم الاسم الحقيقي اللي جلبناه من جدول users
        status = "✅ " if m_id in selected_list else ""
        # الحماية: نمرر owner_id في نهاية الكولباك
        kb.insert(InlineKeyboardButton(
            f"{status}{member['name']}", 
            callback_data=f"toggle_mem_{m_id}_{owner_id}"
        ))
    
    if selected_list:
        # زر محمي تماماً لا ينتقل إلا بآيدي صاحب الجلسة
        kb.add(InlineKeyboardButton(
            f"➡️ تم اختيار ({len(selected_list)}) .. عرض أقسامهم", 
            callback_data=f"go_to_cats_step_{owner_id}"
        ))
    
    kb.add(InlineKeyboardButton("🔙 رجوع", callback_data=f"setup_quiz_{owner_id}"))
    await message.edit_text("👥 <b>أقسام الأعضاء المبدعين:</b>\nاختر المبدعين لعرض أقسامهم:", reply_markup=kb, parse_mode="HTML")

# 2. دالة عرض المجلدات (نظام البوت الرسمي الجديد)
async def render_folders_list(message, eligible_folders, selected_folders, owner_id):
    kb = InlineKeyboardMarkup(row_width=2)
    for folder in eligible_folders:
        f_id = str(folder['id'])
        status = "✅ " if f_id in selected_folders else ""
        kb.insert(InlineKeyboardButton(
            f"{status}{folder['name']}", 
            callback_data=f"toggle_folder_{f_id}_{owner_id}"
        ))
    
    if selected_folders:
        kb.add(InlineKeyboardButton(
            f"➡️ تم اختيار ({len(selected_folders)}) .. عرض الأقسام", 
            callback_data=f"confirm_folders_{owner_id}"
        ))
    
    kb.add(InlineKeyboardButton("🔙 رجوع", callback_data=f"setup_quiz_{owner_id}"))
    await message.edit_text("🗂️ <b>مجلدات البوت الرسمية:</b>\nاختر المجلدات المطلوبة:", reply_markup=kb, parse_mode="HTML")

# 3. دالة عرض الأقسام (محمية من المبعسسين)
async def render_categories_list(message, eligible_cats, selected_cats, owner_id):
    kb = InlineKeyboardMarkup(row_width=2)
    for cat in eligible_cats:
        cat_id_str = str(cat['id'])
        status = "✅ " if cat_id_str in selected_cats else ""
        kb.insert(InlineKeyboardButton(
            f"{status}{cat['name']}", 
            callback_data=f"toggle_cat_{cat_id_str}_{owner_id}"
        ))
    
    if selected_cats:
        # زر محمي: يمنع المبعسس من الانتقال لواجهة الإعدادات النهائية
        kb.add(InlineKeyboardButton(
            f"➡️ تم اختيار ({len(selected_cats)}) .. الإعدادات", 
            callback_data=f"final_quiz_settings_{owner_id}"
        ))
    
    kb.add(InlineKeyboardButton("🔙 رجوع", callback_data=f"setup_quiz_{owner_id}"))
    await message.edit_text("📂 <b>اختر الأقسام المطلوبة:</b>", reply_markup=kb, parse_mode="HTML")


    # ==========================================
# --- [  قالب اعدادات المسابقه ] ---
# ==========================================
async def render_final_settings_panel(message, data, owner_id):
    """لوحة إعدادات أنيقة بنظام التدوير (Cycling)"""
    q_time = data.get('quiz_time', 15)
    q_count = data.get('quiz_count', 10)
    q_mode = data.get('quiz_mode', 'السرعة ⚡')
    q_style = data.get('quiz_style', 'اختيارات 📊') 
    is_hint = data.get('quiz_hint_bool', False)
    is_broadcast = data.get('is_broadcast', False)
    
    q_hint_text = "مفعل ✅" if is_hint else "معطل ❌"
    q_scope_text = "إذاعة عامة 🌐" if is_broadcast else "مسابقة داخلية 📍"
    
    text = (
       f"⚙️ **لوحة إعدادات المسابقة**\n"
       f"❃┅┅┅┄┄┄┈•❃•┈┄┄┄┅┅┅❃\n"
       f"📊 عدد الأسئلة: `{q_count}`\n"
       f"⏳ المهلة: `{q_time} ثانية`\n"
       f"🎨 العرض: `{q_style}`\n"
       f"🔖 النظام: `{q_mode}`\n"
       f"📡 النطاق: `{q_scope_text}`\n"
       f"💡 التلميح: `{q_hint_text}`\n"
       f"❃┅┅┅┄┄┄┈•❃•┈┄┄┄┅┅┅❃\n"
       f"⚠️ *هذه الإعدادات خاصة بـ {data.get('owner_name', 'المنظم')} فقط*"
    )

    kb = InlineKeyboardMarkup(row_width=2)
    
    # أزرار التدوير (كل زر يغير قيمته عند الضغط)
    kb.row(
        InlineKeyboardButton(f"📊 الأسئلة: {q_count}", callback_data=f"cyc_cnt_{owner_id}"),
        InlineKeyboardButton(f"⏱️ الوقت: {q_time}ث", callback_data=f"cyc_time_{owner_id}")
    )
    
    kb.row(InlineKeyboardButton(f"🎨 العرض: {q_style}", callback_data=f"cyc_style_{owner_id}"))
    
    kb.row(
        InlineKeyboardButton(f"🔖 {q_mode}", callback_data=f"cyc_mode_{owner_id}"),
        InlineKeyboardButton(f"💡 التلميح: {q_hint_text}", callback_data=f"cyc_hint_{owner_id}")
    )
    
    kb.row(InlineKeyboardButton(f"📡 النطاق: {q_scope_text}", callback_data=f"tog_broad_{owner_id}"))
    
    kb.row(InlineKeyboardButton("🚀 حفظ وبدء المسابقة 🚀", callback_data=f"start_quiz_{owner_id}"))
    kb.row(InlineKeyboardButton("❌ إلغاء", callback_data=f"setup_quiz_{owner_id}"))
    
    await message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
# ==========================================
# 3. دوال الفحص الأمني والمحركات (Security Helpers & Engines)
# ==========================================
async def get_group_status(chat_id):
    """فحص حالة تفعيل المجموعة في الجدول الموحد الجديد groups_hub"""
    try:
        res = supabase.table("groups_hub").select("status").eq("group_id", chat_id).execute()
        return res.data[0]['status'] if res.data else "not_found"
    except Exception as e:
        logging.error(f"Error checking group status: {e}")
        return "error"
# ==========================================
# --- [  دالة فحص قبل الإعلان ] ---
# ==========================================
async def security_checkpoint(m: types.CallbackQuery or types.Message):
    # ميزة: التعامل مع الضغطة (Callback) أو الرسالة (Message)
    cid = m.message.chat.id if isinstance(m, types.CallbackQuery) else m.chat.id
    uid = m.from_user.id
    c_type = m.message.chat.type if isinstance(m, types.CallbackQuery) else m.chat.type

    # 1️⃣ فحص نوع الدردشة (المنع من الخاص)
    if c_type == 'private':
        await m.answer("⚠️ الإذاعة العامة تعمل فقط داخل المجموعات المفعّلة.", show_alert=True)
        return False

    # 2️⃣ فحص تفعيل المجموعة (الشرط الأساسي)
    try:
        res_group = supabase.table("groups_hub").select("status").eq("group_id", cid).execute()
        if not res_group.data or res_group.data[0]['status'] != 'active':
            await m.answer("🚫 هذه المجموعة غير مفعّلة في نظام أثير. لا يمكن تشغيل المسابقات العامة هنا.", show_alert=True)
            return False
    except Exception as e:
        logging.error(f"Error in Group Check: {e}")
        return False

    # 3️⃣ فحص الأهلية (مشرف أو خبير إجابات)
    is_admin = False
    try:
        member = await bot.get_chat_member(cid, uid)
        if member.status in ['creator', 'administrator']:
            is_admin = True
    except: pass

    if is_admin:
        return True # المشرف مسموح له دائماً في المجموعة المفعلة

    # إذا لم يكن مشرفاً، نبحث في سجل الإجابات
    try:
        res_user = supabase.table("users_global_profile").select("correct_answers_count").eq("user_id", uid).execute()
        if res_user.data:
            ans_count = res_user.data[0].get('correct_answers_count', 0)
            if ans_count >= 150:
                return True # لاعب خبير مسموح له
            else:
                await m.answer(f"⚠️ عذراً! يجب أن تكون مشرفاً أو لديك 150 إجابة صحيحة (رصيدك: {ans_count}).", show_alert=True)
                return False
        else:
            await m.answer("⚠️ لم يتم العثور على ملفك الشخصي. شارك في المسابقات أولاً!", show_alert=True)
            return False
    except Exception as e:
        logging.error(f"Error in User Check: {e}")
        return False
        
async def run_visual_countdown(group_msgs, kb, base_info):
    """دالة العد التنازلي البصري - آخر 10 ثوانٍ 🔥"""
    timer_emojis = ["🔟", "9️⃣", "8️⃣", "7️⃣", "6️⃣", "5️⃣", "4️⃣", "3️⃣", "2️⃣", "1️⃣"]
    
    for emoji in timer_emojis:
        # نص الإعلان مع تحديث التوقيت فقط
        text = f"{base_info}\n\n⏳ **ستبدأ المسابقة بعد:** {emoji}\n👈 إن كنت لا تريد المشاركة اضغط إلغاء أدناه."
        
        edit_tasks = []
        for cid, mid in group_msgs.items():
            if cid not in cancelled_groups:
                edit_tasks.append(bot.edit_message_text(text, cid, mid, reply_markup=kb, parse_mode="Markdown"))
        
        await asyncio.gather(*edit_tasks, return_exceptions=True)
        await asyncio.sleep(1)

async def start_broadcast_process(c: types.CallbackQuery, quiz_id: int, owner_id: int):
    try:
        # 1. المندوب يجلب بيانات المسابقة (قراءة فقط من saved_quizzes)
        res_q = supabase.table("saved_quizzes").select("*").eq("id", quiz_id).single().execute()
        q_data = res_q.data
        if not q_data: return await c.answer("❌ تعذر جلب بيانات المسابقة")

        # 2. المندوب يجلب المجموعات النشطة من groups_hub ليوزع عليها الإعلان
        groups_res = supabase.table("groups_hub").select("group_id").eq("status", "active").execute()
        if not groups_res.data: return await c.answer("⚠️ لا توجد مجموعات نشطة!")

        all_chats = [g['group_id'] for g in groups_res.data]
        cancelled_groups.clear() 

        owner_name = c.from_user.first_name
        base_info = (
            f"**إعلان: مسابقة عامة منطلقة !** ™️\n"
            f"━━━━━━━━━━━━━━\n"
            f"🏆 المسابقة: **{q_data.get('quiz_name', 'تحدي جديد')}**\n"
            f"📂 القسم: **{q_data.get('category_name', 'عام')}**\n"
            f"👤 المنظم: **{owner_name}**\n"
            f"━━━━━━━━━━━━━━"
        )

        # 3. المندوب يوزع رسائل التحضير (فتح باب الانضمام)
        group_msgs = {}
        kb = InlineKeyboardMarkup().add(
            InlineKeyboardButton("🚫 إلغاء المسابقة في مجموعتنا", callback_data=f"cancel_quiz_{quiz_id}")
        )

        for cid in all_chats:
            try:
                msg = await bot.send_message(cid, f"{base_info}\n\n🛰️ **جاري تحضير الإذاعة العالمية...**", 
                                           parse_mode="Markdown", reply_markup=kb)
                group_msgs[cid] = msg.message_id
            except: continue

        # 4. مرحلة الانتظار (فرصة للمجموعات لتقرر البقاء أو الرفض)
        await asyncio.sleep(5)
        # هنا يتم تشغيل العد التنازلي البصري (اختياري)
        # await run_visual_countdown(group_msgs, kb, base_info)

        # 🚀 [ المرحلة الجوهرية: الفرز ] 🚀
        # المندوب يحدد القائمة النهائية للراغبين باللعب
        final_groups = [cid for cid in group_msgs if cid not in cancelled_groups]

        if final_groups:
            # تحديث بَصري للمجموعات المنضمة
            launch_tasks = [
                bot.edit_message_text(f"{base_info}\n\n🚀 **تـم الانـطـلاق الآن! جاري التسجيل..**", cid, mid, parse_mode="Markdown") 
                for cid, mid in group_msgs.items() if cid in final_groups
            ]
            await asyncio.gather(*launch_tasks, return_exceptions=True)

            # أضف الـ ID يدوياً قبل إرساله للمحرك لضمان عدم ضياعه
            q_data['owner_id'] = c.from_user.id 
            asyncio.create_task(engine_global_broadcast(final_groups, q_data, owner_name))
            logging.info(f"📡 المندوب سلم {len(final_groups)} مجموعة للمحرك.")

        else:
            await bot.send_message(owner_id, "⚠️ تم إلغاء المسابقة لعدم وجود مجموعات منضمة.")

        # 7. التنظيف النهائي لرسائل الإعلان
        await asyncio.sleep(2)
        for cid, mid in group_msgs.items():
            try: await bot.delete_message(cid, mid)
            except: pass

    except Exception as e:
        logging.error(f"🚨 General Broadcast Error: {e}")
        
# --- [ 1. الدوال الخدمية - الربط مع سوبابيس ] ---
async def get_user_full_data(user_id: int):
    """جلب بيانات اللاعب من جدول users_global_profile"""
    try:
        # التأكد من تحويل الـ ID لرقم صحيح ليتوافق مع BigInt
        res = supabase.table("users_global_profile").select("*").eq("user_id", int(user_id)).execute()
        return res.data[0] if res.data else None
    except Exception as e:
        logging.error(f"خطأ في جلب بيانات الجدول users_global_profile: {e}")
        return None

async def format_profile_card(user_data: dict, user_id: int):
    """
    تنسيق البطاقة الفخمة - نسخة ياسر المطورة 2026
    تم إضافة نظام الدول وتحديث الأزرار التفاعلية.
    """
    p = user_data
    ans_count = p.get('correct_answers_count', 0)
    
    # --- [ 1. منطق الرتب والتقدم ] ---
    ranks_map = [
        ("طالب مبتدئ", 100), ("طالب ثانوية", 250), ("طالب جامعي", 500),
        ("بروفيسور", 1000), ("عالم عبقري", 2000), ("أسطورة المعرفة", 5000)
    ]
    
    current_rank, next_rank_name, target_pts, prev_pts = "طالب مبتدئ", "القمة", 5000, 0
    for i, (name, limit) in enumerate(ranks_map):
        if ans_count <= limit:
            current_rank = name
            next_rank_name = ranks_map[i+1][0] if i+1 < len(ranks_map) else "القمة"
            target_pts, prev_pts = limit, (ranks_map[i-1][1] if i > 0 else 0)
            break

    percentage = min(100, max(0, ((ans_count - prev_pts) / (target_pts - prev_pts)) * 100))
    progress_bar = "🟢" * int(percentage // 10) + "⚪" * (10 - int(percentage // 10))

    # --- [ 2. معالجة البيانات المعقدة (JSON) ] ---
    def parse_json(data):
        if isinstance(data, str):
            import json
            try: return json.loads(data)
            except: return {}
        return data or {}

    stats = parse_json(p.get('category_stats'))
    cards = parse_json(p.get('cards_inventory'))
    titles = p.get('titles', []) 
    inventory = p.get('inventory', []) 

    # --- [ 3. بناء نص البطاقة النهائي ] ---
    card = f"<b>    👤 : بـروفـايـل الـمـتـمـيـز 👤</b>\n"
    card += "<b>— — — — — — — — — — — —</b>\n"
    card += f"🆔 <b>:</b> الاسم ⇠ <a href='tg://user?id={user_id}'>{p.get('user_name', 'مشارك جديد')}</a>\n"
    
    # التعديل الجديد: سطر الدولة والعلم
    country = p.get('country_name', 'غير محدد')
    flag = p.get('country_flag', '🌐')
    card += f"🌍 <b>:</b> الدولة ⇠ <b>{country} {flag}</b>\n"
    
    card += f"💳 <b>:</b> الحساب ⇠ <code>{p.get('bank_account', '----')}</code>\n"
    card += f"🎓 <b>:</b> الرتبة ⇠ <b>{current_rank}</b>\n"
    card += f"🎖 <b>:</b> التخصص ⇠ <b>{p.get('specialty_title', 'هاوي')}</b>\n"
    
    # --- [ بقية أقسام البطاقة كما هي ] ---
    if titles:
        card += "<b>— — — — — — — — — — — —</b>\n"
        card += "<b>👑 : الألـقـاب الـمـلـكـيـة :</b>\n"
        for title in titles:
            card += f"  ⇠ <code>{title}</code>\n"
    
    card += "<b>— — — — — — — — — — — —</b>\n"
    card += f"📈 <b>: التقدم لـ ({next_rank_name}) :</b>\n"
    card += f"{progress_bar} <code>{int(percentage)}%</code>\n"
    card += f"🎯 <b>:</b> المتبقي ⇠ <code>{max(0, target_pts - ans_count)}</code> إجابة\n"
    card += "<b>— — — — — — — — — — — —</b>\n"
    
    card += f"💰 <b>:</b> المحفظة ⇠ <code>{p.get('wallet', 0)}</code> ن\n"
    card += f"🧠 <b>:</b> الذكاء ⇠ <code>{p.get('iq_score', 0)}% IQ</code>\n"
    card += f"🏆 <b>:</b> الفوز العام ⇠ <code>{p.get('total_wins', 0)}</code>\n"
    card += f"🔥 <b>:</b> فوز خاص ⇠ <code>{p.get('special_wins', 0)}</code>\n"
    card += f"✅ <b>:</b> الإجمالي ⇠ <code>{ans_count}</code> إجابة\n"
    card += "<b>— — — — — — — — — — — —</b>\n"
    
    card += "<b>🃏 : مـخـزن الـكـروت الـمـلـكـي :</b>\n"
    card += f"🔍 <b>:</b> كرت إظهار حرف ⇠ [ <code>{cards.get('letter', 0)}</code> ]\n"
    card += f"💡 <b>:</b> كرت التلميح الكامل ⇠ [ <code>{cards.get('full', 0)}</code> ]\n"
    card += f"⏱️ <b>:</b> كرت زيادة الوقت ⇠ [ <code>{cards.get('time', 0)}</code> ]\n"
    card += f"🎯 <b>:</b> كرت كشف الإجابة ⇠ [ <code>{cards.get('reveal', 0)}</code> ]\n"
    card += f"💰 <b>:</b> كرت المضاعفة x2 ⇠ [ <code>{cards.get('double', 0)}</code> ]\n"
    card += f"🛡️ <b>:</b> كرت حماية الدرع ⇠ [ <code>{cards.get('shield', 0)}</code> ]\n"
    card += "<b>— — — — — — — — — — — —</b>\n"

    if inventory:
        card += "<b>📦 : الـمـقـتـنـيـات والـنـوادر :</b>\n"
        for item in inventory:
            card += f"  ⇠ <code>{item}</code>\n"
        card += "<b>— — — — — — — — — — — —</b>\n"

    return card

# 1. لوحة البروفايل (الزر الرئيسي)
def get_profile_keyboard(user_id):
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    keyboard = InlineKeyboardMarkup(row_width=2)
    keyboard.add(
       # نستخدم set_country لفتح قائمة الدول
       InlineKeyboardButton("🚩 : إضافة دولتي", callback_data=f"set_country_{user_id}"),
       InlineKeyboardButton("❌ : إغلاق", callback_data="close_card")
    )
    return keyboard

# 2. لوحة اختيار الدول
def get_countries_keyboard(user_id: int):
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    keyboard = InlineKeyboardMarkup(row_width=3) 
    
    countries = [
        ("اليمن", "🇾🇪"), ("السعودية", "🇸🇦"), ("مصر", "🇪🇬"), 
        ("الإمارات", "🇦🇪"), ("الكويت", "🇰🇼"), ("قطر", "🇶🇦"), 
        ("عمان", "🇴🇲"), ("البحرين", "🇧🇭"), ("العراق", "🇮🇶"), 
        ("الأردن", "🇯🇴"), ("فلسطين", "🇵🇸"), ("سوريا", "🇸🇾"), 
        ("لبنان", "🇱🇧"), ("المغرب", "🇲🇦"), ("تونس", "🇹🇳"), 
        ("الجزائر", "🇩🇿"), ("ليبيا", "🇱🇾"), ("السودان", "🇸🇩"), 
        ("الصومال", "🇸🇴"), ("موريتانيا", "🇲🇷"), ("جيبوتي", "🇩🇯"), 
        ("جزر القمر", "🇰🇲")
    ]
    
    buttons = []
    for name, flag in countries:
        # sv_c اختصار لـ save_country لتقليل حجم البيانات في الـ Callback
        callback_str = f"sv_c_{name}_{flag}_{user_id}"
        buttons.append(InlineKeyboardButton(text=f"{name} {flag}", callback_data=callback_str))
    
    keyboard.add(*buttons)
    keyboard.row(InlineKeyboardButton(text="⬅️ رجوع للبروفايل", callback_data=f"back_to_profile_{user_id}"))
    return keyboard
# ============================================================
# دالة معالجة التحويل البنكي - نسخة ياسر 2026
# ============================================================
async def process_bank_transfer(sender_id, amount, receiver_id=None, receiver_acc=None):
    try:
        # 1. جلب بيانات الراسل
        sender_res = supabase.table("users_global_profile").select("*").eq("user_id", sender_id).single().execute()
        if not sender_res.data: return "❌ ليس لديك حساب بنكي مسجل."
        
        sender = sender_res.data
        if sender['wallet'] < amount:
            return f"❌ رصيدك غير كافٍ. رصيدك الحالي: {sender['wallet']} ن"

        # 2. تحديد المستلم (إما عن طريق ID أو رقم الحساب)
        query = supabase.table("users_global_profile").select("*")
        if receiver_id:
            query = query.eq("user_id", receiver_id)
        else:
            query = query.eq("bank_account", receiver_acc)
        
        receiver_res = query.single().execute()
        if not receiver_res.data: return "❌ تعذر العثور على حساب المستلم."
        
        receiver = receiver_res.data
        if receiver['user_id'] == sender_id: return "❌ لا يمكنك التحويل لنفسك!"

        # 3. تنفيذ العملية (خصم وإضافة)
        fee = int(amount * 0.02)  # عمولة 5%
        net_amount = amount - fee

        # تحديث الراسل
        supabase.table("users_global_profile").update({"wallet": sender['wallet'] - amount}).eq("user_id", sender_id).execute()
        # تحديث المستلم
        supabase.table("users_global_profile").update({"wallet": receiver['wallet'] + net_amount}).eq("user_id", receiver['user_id']).execute()

        # 4. تنسيق قالب النجاح الفخم
        msg = f"<b>🏧 تمـت عـمـلـيـة الـتـحـويـل بـنـجـاح</b>\n"
        msg += f"<b>— — — — — — — — — — — —</b>\n"
        msg += f"👤 <b>الـمـسـتـلـم :</b> {receiver.get('user_name', 'غير معروف')}\n"
        msg += f"💳 <b>الـحـسـاب :</b> <code>#{receiver['bank_account']}</code>\n"
        msg += f"💰 <b>الـمـبـلـغ الـمـرسـل :</b> <code>{amount}</code> ن\n"
        msg += f"📉 <b>الـعـمـولـة (2%) :</b> <code>{fee}</code> ن\n"
        msg += f"✅ <b>الـصـافـي للـمـسـتـلـم :</b> <code>{net_amount}</code> ن\n"
        msg += f"<b>— — — — — — — — — — — —</b>\n"
        msg += f"✨ <i>شكراً لاستخدامك خدمات بنك زدني</i>"
        return msg

    except Exception as e:
        print(f"Transfer Error: {e}")
        return "⚠️ حدث خطأ فني أثناء التحويل."
        
# --- [ 1. قاعدة بيانات الأصناف ] ---
ITEMS_DB = {
    # --- [ 👑 الألقاب الملكية - ألقاب الهيبة ] ---
    "royal": {
        "r1": {"name": "👑 الملك", "price": 50000},
        "r2": {"name": "🎩 الإمبراطور", "price": 100000},
        "r3": {"name": "💎 الأسطورة", "price": 200000},
        "r4": {"name": "🌟 الجنرال", "price": 300000},
        "r5": {"name": "⚔️ الفارس", "price": 10000},
        "r6": {"name": "🛡️ الحارس", "price": 5000},
        "r7": {"name": "⚜️ النبيل", "price": 150000},
        "r8": {"name": "🐲 التنين", "price": 500000},
        "r9": {"name": "⚡ البرق", "price": 40000},
        "r10": {"name": "🦅 الصقر الملكي", "price": 25000},
        "r11": {"name": "🔥 البركان", "price": 18000},
        "r12": {"name": "🔱 القائد الأعلى", "price": 60000},
        "r13": {"name": "🌑 سيد الظلام", "price": 350000},
        "r14": {"name": "🦁 أسد الفلوجة", "price": 120000},
        "r15": {"name": "🐅 النمر", "price": 90000},
        "r16": {"name": "⚔️ السيف الصارم", "price": 7500},
        "r17": {"name": "🏹 القناص", "price": 6500},
        "r18": {"name": "🌌 حامي المجرة", "price": 10000},
        "r19": {"name": "🌪️ الإعصار", "price": 2200},
        "r20": {"name": "🗿 العملاق", "price": 4500},
        # (يمكنك إضافة المزيد هنا بنفس النمط حتى تصل لـ 100)
    },

    # --- [ 🌸 الألقاب البناتية - الرقة والجمال ] ---
    "girls": {
        "g1": {"name": "🎀 الأميرة", "price": 50000},
        "g2": {"name": "👑 الملكة", "price": 100000},
        "g3": {"name": "🦋 الفراشة", "price": 5000},
        "g4": {"name": "🌸 الوردة", "price": 3000},
        "g5": {"name": "🌙 قمر الزمان", "price": 150000},
        "g6": {"name": "💎 الجوهرة", "price": 250000},
        "g7": {"name": "✨ النجمة", "price": 10000},
        "g8": {"name": "🍭 السكرة", "price": 2000},
        "g9": {"name": "🎵 القيثارة", "price": 80000},
        "g10": {"name": "🌬️ النسمة", "price": 15000},
        "g11": {"name": "🧸 الدلوعة", "price": 7000},
        "g12": {"name": "🌹 الياسمينة", "price": 12000},
        "g13": {"name": "🎻 عازفة الأمل", "price": 95000},
        "g14": {"name": "🌊 حورية البحر", "price": 300000},
        "g15": {"name": "💎 الألماسة", "price": 500000},
        "g16": {"name": "🏹 الصيادة", "price": 40000},
        "g17": {"name": "❄️ ملكة الثلج", "price": 200000},
        "g18": {"name": "🍓 التوتة", "price": 5000},
        "g19": {"name": "🕊️ الحمامة", "price": 30000},
        "g20": {"name": "🧚 الجنية", "price": 180000},
    },

    # --- [ 🌹 هدايا وورود ] ---
    "gifts": {
        "rosered": {"name": "🌹 باقة ورد أحمر", "price": 1000},
        "tulip": {"name": "🌷 زهرة التوليب", "price": 1200},
        "bouquet": {"name": "💐 الباقة الملكية", "price": 5000},
        "sunflower": {"name": "🌻 إشراقة أمل", "price": 1500},
        "jasmine": {"name": "⚪ ياسمين الشام", "price": 1100},
        "choc": {"name": "🍫 صندوق شوكولا", "price": 2000},
        "giftb": {"name": "🎁 صندوق المفاجآت", "price": 3000},
        "ring": {"name": "💍 خاتم الألماس", "price": 20000},
        "perfume": {"name": "🧴 عطر فرنسي", "price": 8000},
        "watch": {"name": "⌚ ساعة رولكس", "price": 45000},
        "goldr": {"name": "💍 خاتم ذهب عيار 21", "price": 15000},
        "teddy": {"name": "🧸 دبدوب كبير", "price": 4000},
        "cake": {"name": "🎂 كيكة الاحتفال", "price": 6000},
        "iphone": {"name": "📱 آيفون 15 بروماكس", "price": 50000},
        "laptop": {"name": "💻 لابتوب قيمنق", "price": 70000},
    },

    # --- [ 🥂 رفاهية المليارديرات ] ---
    "rare": {
        "goldbar": {"name": "🧱 سبيكة ذهب", "price": 100000},
        "luxurycar": {"name": "🏎️ فيراري", "price": 500000},
        "privatejet": {"name": "🛩️ طائرة خاصة", "price": 2000000},
        "yacht": {"name": "🛥️ يخت ملكي", "price": 5000000},
        "island": {"name": "🏝️ جزيرة خاصة", "price": 10000000},
        "crowndiamond": {"name": "👑 التاج الماسي", "price": 50000000},
        "satellite": {"name": "🛰️ قمر صناعي", "price": 100000000},
        "spaceship": {"name": "🚀 سفينة فضاء", "price": 500000000},
        "pyramid": {"name": "📐 هرم خاص", "price": 150000000},
        "oilwell": {"name": "🛢️ بئر نفط", "price": 80000000},
        "footballclub": {"name": "⚽ نادي رياضي", "price": 120000000},
        "moonland": {"name": "🌑 قطعة أرض على القمر", "price": 1000000000},
        "marsbase": {"name": "🚀 قاعدة على المريخ", "price": 5000000000},
        "flat": {"name": "🏢 شقة فاخرة", "price": 30000},
        "villa": {"name": "🏡 فيلا بمسبح", "price": 150000},
        "palace": {"name": "🏰 قصر منيف", "price": 1000000},
        "tower": {"name": "🏙️ ناطحة سحاب", "price": 2000000},
        "hotel": {"name": "🏨 فندق 7 نجوم", "price": 4000000},
        "city": {"name": "🌆 مدينة كاملة", "price": 100000000},
        "stadium": {"name": "🏟️ ملعب رياضي", "price": 60000000},
        "bank": {"name": "🏦 بنك مركزي", "price": 900000000},
        "mall": {"name": "🛍️ مول تجاري", "price": 25000000},
        "museum": {"name": "🏛️ متحف تاريخي", "price": 12000000},
        "factory": {"name": "🏭 مصنع ضخم", "price": 18000000},
        "village": {"name": "🏘️ قرية سياحية", "price": 50000000},
    },
    
    # --- [ 🃏 كروت اللعب ] ---
    "cards": {
        "letter": {"name": "🔍 كرت إظهار حرف", "price": 1500},
        "full": {"name": "💡 كرت التلميح", "price": 3000},
        "time": {"name": "⏱️ كرت زيادة الوقت", "price": 2500},
        "reveal": {"name": "🎯 كرت كشف الإجابة", "price": 10000},
        "double": {"name": "💰 كرت مضاعفة المبلغ x2", "price": 7000},
        "shield": {"name": "🛡️ كرت الحماية", "price": 5000}
    },
}
# --- [ 2. دالة تنسيق واجهة المتجر ] ---
async def format_shop_bazaar_card(user_wallet: int):
    """تجهيز القالب النصي الفخم للمتجر"""
    msg =  "<b>   🛒 : الـمـتـجـر الـعـالـمـي الـكـبـيـر 🛒</b>\n"
    msg += "<b>━━━━━━━━━━━━━━━━━━</b>\n"
    msg += f"💰: <b>: رصيدك الحالي ⇠ <code>{user_wallet}</code> نقطة</b>\n"
    msg += "<b>━━━━━━━━━━━━━━━━━━</b>\n\n"
    msg += "<b>🔹 : تصفح الأقسام عبر الأزرار :</b>\n"
    msg += "👑 ⇠ ألقاب ملكية | 🌸 ⇠ ألقاب بناتي\n"
    msg += "💐 ⇠ هدايا وورود | ⚔️ ⇠ مقتنيات نادرة\n"
    msg += "🃏 ⇠ كروت مساعدة\n\n"
    msg += "<b>━━━━━━━━━━━━━━━━━━</b>\n"
    msg += "✅ : اختر القسم الذي ترغب بتصفحه بالأسفل"
    return msg
# --- [ 3. دوال الأزرار (Keyboards) المنسقة ] ---
def get_shop_main_keyboard(user_id):
    keyboard = InlineKeyboardMarkup(row_width=2)
    
    # استخدمنا open_cat_ لكي يقرأها المعالج مباشرة
    # وأضفنا _ID في النهاية لحماية "البعسسة"
    keyboard.add(
        InlineKeyboardButton("👑 : الألقاب الملكية", callback_data=f"open_cat_royal_{user_id}"),
        InlineKeyboardButton("🌸 : الألقاب البناتية", callback_data=f"open_cat_girls_{user_id}")
    )
    keyboard.add(
        InlineKeyboardButton("💐 : الورود والهدايا", callback_data=f"open_cat_gifts_{user_id}"),
        InlineKeyboardButton("⚔️ : مقتنيات نادرة", callback_data=f"open_cat_rare_{user_id}")
    )
    keyboard.add(
        InlineKeyboardButton("🃏 : كروت اللعب", callback_data=f"open_cat_cards_{user_id}"),
        InlineKeyboardButton("❌ : إغلاق المتجر", callback_data=f"close_card_{user_id}")
    )
    
    return keyboard

# --- [ 3.5 دالة توليد أزرار المنتجات داخل الأقسام ] ---
def get_products_keyboard(category, user_id):
    """توليد أزرار المنتجات (الأسماء فقط) مع حماية الآيدي"""
    keyboard = InlineKeyboardMarkup(row_width=2)
    
    # جلب قائمة المنتجات الخاصة بالقسم المختار
    products = ITEMS_DB.get(category, {})
    
    for p_id, p_info in products.items():
        # تعديلك المطلوب: عرض اسم السلعة فقط بدون السعر
        btn_text = f"{p_info['name']}"
        
        # داتا الزر المشفرة (تأكد أن p_id هو المفتاح البرمجي وليس الاسم الطويل)
        btn_data = f"buy_{p_id}_{category}_{user_id}"
        
        keyboard.insert(InlineKeyboardButton(btn_text, callback_data=btn_data))
    
    # زر العودة
    keyboard.add(InlineKeyboardButton("🔙 : الـعـودة لـلـقـائمة", callback_data=f"back_to_shop_{user_id}"))
    
    return keyboard
# ============================================================
# دالة إنشاء لوحة اختيار الدول العربية (22 دولة)
# الوظيفة: توليد أزرار بأسماء وأعلام الدول لتحديث بيانات البروفايل
# ============================================================
def get_countries_keyboard(user_id: int):
    keyboard = InlineKeyboardMarkup(row_width=3) 
    
    countries = [
        ("اليمن", "🇾🇪"), ("السعودية", "🇸🇦"), ("مصر", "🇪🇬"), 
        ("الإمارات", "🇦🇪"), ("الكويت", "🇰🇼"), ("قطر", "🇶🇦"), 
        ("عمان", "🇴🇲"), ("البحرين", "🇧🇭"), ("العراق", "🇮🇶"), 
        ("الأردن", "🇯🇴"), ("فلسطين", "🇵🇸"), ("سوريا", "🇸🇾"), 
        ("لبنان", "🇱🇧"), ("المغرب", "🇲🇦"), ("تونس", "🇹🇳"), 
        ("الجزائر", "🇩🇿"), ("ليبيا", "🇱🇾"), ("السودان", "🇸🇩"), 
        ("الصومال", "🇸🇴"), ("موريتانيا", "🇲🇷"), ("جيبوتي", "🇩🇯"), 
        ("جزر القمر", "🇰🇲")
    ]
    
    buttons = []
    for name, flag in countries:
        # sv_c اختصار لـ save_country لتقليل حجم البيانات
        callback_str = f"sv_c_{name}_{flag}_{user_id}"
        buttons.append(InlineKeyboardButton(text=f"{name} {flag}", callback_data=callback_str))
    
    keyboard.add(*buttons)
    
    # زر الرجوع للبروفايل
    keyboard.row(
        InlineKeyboardButton(text="⬅️ رجوع للبروفايل", callback_data=f"back_to_profile_{user_id}")
    )
    
    return keyboard
# ============================================================
# دالة عرض الألقاب والمقتنيات (الخزنة الملكية)
# الوظيفة: تنسيق قائمة الألقاب والنوادر بشكل فخم ومنظم
# ============================================================
def format_vault_display(user_name: str, titles: list, inventory: list):
    """
    تنسيق عرض المقتنيات والألقاب بشكل فخم.
    يتم عرض كل عنصر في سطر مستقل مع أيقونة مميزة.
    """
    
    # --- [ 1. ترويسة الخزنة ] ---
    display = f"<b>🏛️ : خـزنـة الـمـتـمـيـز : {user_name}</b>\n"
    display += "<b>— — — — — — — — — — — —</b>\n\n"

    # --- [ 2. قسم الألقاب الملكية ] ---
    display += "<b>👑 : الألـقـاب والـرتـب الـشـرفـيـة :</b>\n"
    if titles and len(titles) > 0:
        for title in titles:
            # استخدام كود مونو لتبريز اللقب
            display += f"  ⇠ <code>{title}</code>\n"
    else:
        display += "  ⇠ <i>لا توجد ألقاب مسجلة حالياً</i>\n"
    
    display += "\n<b>— — — — — — — — — — — —</b>\n\n"

    # --- [ 3. قسم المقتنيات والنوادر ] ---
    display += "<b>📦 : الـمـقـتـنـيـات والـمـوارد الـنـادرة :</b>\n"
    if inventory and len(inventory) > 0:
        for item in inventory:
            # إضافة أيقونة الصندوق لكل مقتنى
            display += f"  🎁 ⇠ <code>{item}</code>\n"
    else:
        display += "  ⇠ <i>المخزن فارغ تماماً</i>\n"

    # --- [ 4. التذييل ] ---
    display += "\n<b>— — — — — — — — — — — —</b>\n"
    display += "✨ <i>استمر في التميز لزيادة مقتنياتك</i>"

    return display

# ==========================================
async def sync_quiz_to_supabase(chat_id):
    quiz = active_quizzes.get(chat_id)
    if not quiz: return

    # تحويل البيانات لشكل يفهمه سوبابيس
    update_data = {
        "poll_options": quiz['options'],
        "votes_results": {str(k): len(v) for k, v in quiz['votes'].items()},
        "voter_list": quiz['user_choices']
    }

    try:
        supabase.table("active_quizzes").update(update_data).eq("chat_id", chat_id).eq("is_active", True).execute()
    except Exception as e:
        logging.error(f"❌ خطأ في مزامنة البيانات: {e}")
        

async def get_user_answers_count(user_id: int) -> int:
    try:
        res = supabase.table("users_global_profile").select("correct_answers_count").eq("user_id", user_id).execute()
        if res.data:
            return res.data[0].get("correct_answers_count", 0)
    except Exception as e:
        logging.error(f"❌ فشل جلب عدد الإجابات للمستخدم {user_id}: {e}")
    return 0

# ==========================================
# 4. حالات النظام (FSM States)
# ==========================================
class Form(StatesGroup):
    waiting_for_cat_name = State()
    waiting_for_question = State()
    waiting_for_ans1 = State()
    waiting_for_ans2 = State()
    waiting_for_new_cat_name = State()
    waiting_for_quiz_name = State()
# ==========================================
# حالات نظام التحويل البنكي (Bank FSM)
# ==========================================
class BankTransfer(StatesGroup):
    waiting_for_account = State()  # حالة انتظار رقم الحساب البنكي
    waiting_for_amount = State()   # حالة انتظار إرسال المبلغ

# ==========================================
@dp.message_handler(lambda message: message.text and (message.text.startswith('حسابي') or message.text.startswith('حسابه')))
async def get_user_bank_card(message: types.Message):
    # 1. تحديد المستهدف
    target_user = message.reply_to_message.from_user if message.text.startswith('حسابه') and message.reply_to_message else message.from_user
    
    if message.text.startswith('حسابه') and not message.reply_to_message:
        return await message.reply("⚠️ رد على رسالته أولاً!")

    status_msg = await message.reply("⏳ **جاري مراجعة سجلات بنك زدني...**", parse_mode="Markdown")

    try:
        # 2. استدعاء الدالة (تأكد أنها تعيد القيمة والبيانات الآن)
        result = await generate_zidni_card(target_user.id, bot, supabase)

        if result:
            card_image, user_db = result # تفكيك النتيجة (الصورة + البيانات)
            
            # 3. تجهيز الكابشن الفخم باستخدام بيانات القاعدة
            name = user_db.get('user_name', target_user.full_name)
            acc_num = user_db.get('bank_account', '0000')
            wallet = user_db.get('wallet', 0)
            rank = user_db.get('educational_rank', 'عضو')

            caption = (
                f"🏦 **بطاقة بنك زدني الرسمية**\n"
                f"━━━━━━━━━━━━━━\n"
                f"👤 **الاسم:** {name}\n"
                f"🎖️ **الرتبة:** {rank}\n"
                f"💰 **الرصيد:** {wallet:,} ن\n"
                f"💳 **رقم الحساب ZD:** `{acc_num}`\n"
                f"━━━━━━━━━━━━━━\n"
                f"✨ **حالة الحساب:** نشط ✅"
            )

            await status_msg.delete()
            await message.answer_photo(
                photo=card_image,
                caption=caption,
                parse_mode="Markdown"
            )
        else:
            await status_msg.edit_text("❌ هذا المستخدم غير مسجل في سجلاتنا.")

    except Exception as e:
        print(f"❌ Error: {e}")
        await status_msg.edit_text("⚠️ عذراً، ليس لديك حساب بنكي يرجى لعب مسابقة اولاً ليتم إنشاء حسابك البنكي.")
            
# ==========================================
# 2️⃣ المعالج الرئيسي للأوامر (نظام المكابح الأكاديمي)
# ==========================================
@dp.message_handler(Text(equals=["زدني قف", "زدني توقف"], ignore_case=True), state="*")
async def cmd_stop_quiz(message: types.Message, state: FSMContext):
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    quiz_info = active_quizzes.get(chat_id)
    if not quiz_info:
        await message.reply("🤔 لا توجد مسابقة نشطة حالياً لإيقافها.")
        return

    is_global = quiz_info.get('quiz_type') == 'public' or quiz_info.get('is_global') == True
    db_quiz_id = quiz_info.get('quiz_id')
    
    # 🔍 [ إصلاح الخلل: جلب الهوية الحقيقية للمنظم ]
    # نحاول أولاً من الرام، إذا لم نجدها نسحبها من سوبابيس فوراً
    organizer_id = quiz_info.get('quiz_owner_id')
    
    if not organizer_id and db_quiz_id:
        try:
            res = supabase.table("active_quizzes").select("quiz_owner_id").eq("id", db_quiz_id).execute()
            if res.data:
                organizer_id = res.data[0].get('quiz_owner_id')
        except: pass

    # جلب عدد إجابات اللاعب من جدوله الشخصي للتحقق من الرتبة
    user_answers = await get_user_answers_count(user_id)
    
    has_permission = False
    
    # 🛂 [ نظام الصلاحيات المحدث ]
    if user_id == ADMIN_ID: # أنت المطور (صلاحية مطلقة)
        has_permission = True
    elif organizer_id and user_id == organizer_id: # المنظم (صاحب المسابقة)
        has_permission = True
    else:
        # إذا لم يكن المطور أو المنظم، نطبق شروط الرتبة والإشراف
        if not is_global:
            # --- المسابقة الخاصة ---
            if message.chat.type == "private":
                # في الخاص المنظم فقط (وتحققنا منه فوق)
                pass 
            else:
                # في المجموعات: مشرف المجموعة أو رتبة 150+
                member = await message.chat.get_member(user_id)
                is_admin = member.status in ['creator', 'administrator']
                if is_admin or user_answers >= 150:
                    has_permission = True
        else:
            # --- المسابقة العامة ---
            # في العامة: رتبة 500+ (بما أن المنظم تم التحقق منه فوق)
            if user_answers >= 500:
                has_permission = True

    if not has_permission:
        await message.reply(f"⛔ لا تملك الصلاحية لإيقاف المسابقة.\n(إجاباتك: {user_answers} | المطلوب للعامة: 500 أو أن تكون المنظم)")
        return

    # 🧹 [ التنفيذ والتصفير الشامل ]
    try:
        # 1. إيقاف السجل في سوبابيس (المكبح الرئيسي)
        if db_quiz_id:
            supabase.table("active_quizzes").update({"is_active": False, "is_paused": True}).eq("id", db_quiz_id).execute()

        # 2. تنظيف الرام (محلياً وعالمياً)
        if is_global:
            # نجلب كل المجموعات المشاركة من الرام أو السجل ونمسحها
            participants = quiz_info.get('participants_ids', [])
            for cid in participants:
                active_quizzes.pop(cid, None)
        
        # مسح المجموعة الحالية في كل الأحوال للتأكيد
        active_quizzes.pop(chat_id, None)

        # 3. حذف الاستفتاءات (Polls) المعلقة
        for p_id, p_info in list(active_polls.items()):
            if p_info.get('db_quiz_id') == db_quiz_id:
                active_polls.pop(p_id, None)
                try:
                    await bot.delete_message(p_info['chat_id'], p_info['msg_id'])
                except: pass

        await message.reply("🛑 **تم إيقاف المسابقة  بنجاح!**\nتم الاستجابة لطلب الإيقاف فوراً.", parse_mode="Markdown")

    except Exception as e:
        logging.error(f"❌ خطأ أثناء إيقاف المسابقة: {e}")
        await message.reply("⚠️ حدث خطأ أثناء محاولة إغلاق المسابقة، ولكن تم تصفير الذاكرة المحلية.")

@dp.message_handler(Text(equals=["انسحاب", "خروج", "إلغاء المجموعه"], ignore_case=True), state="*")
async def cmd_withdraw_group(message: types.Message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    quiz_info = active_quizzes.get(chat_id)
    if not quiz_info:
        return 

    # 🛂 [ استخراج هويات القوى ]
    organizer_id = quiz_info.get('quiz_owner_id')
    db_quiz_id = quiz_info.get('quiz_id')
    is_public = quiz_info.get('quiz_type') == 'public'

    # 1️⃣ [ القيد الذهبي: المنظم لا ينسحب ]
    if user_id == organizer_id and user_id != ADMIN_ID:
        return await message.reply(
            "⚠️ **عذراً أيها المنظم!**\n"
            "لا يليق بالقائد أن يغادر الميدان أولاً. أكمل المسابقة أو استخدم (زدني قف)."
        )

    # 2️⃣ [ فحص الصلاحيات ]
    member = await message.chat.get_member(user_id)
    is_admin = member.status in ['creator', 'administrator']
    user_answers = await get_user_answers_count(user_id) 
    
    has_right = (user_id == ADMIN_ID) or is_admin or (user_answers >= 150)
        
    if not has_right:
        return await message.reply(f"🚫 الانسحاب قرار سيادي (مشرف أو 150 إجابة). رصيدك: `{user_answers}`")

    # 🧹 [ تنفيذ الكود العكسي - سحب الفيش وتطهير الجداول ]
    try:
        # 1. القتل الفوري في الرام (المكبح اللحظي)
        active_quizzes.pop(chat_id, None)
        # تحرير المجموعة من قائمة البث النشطة لضمان قدرتها على دخول مسابقة أخرى فوراً
        if chat_id in active_broadcasts:
            active_broadcasts.remove(chat_id)

        # 2. التطهير العميق من سوبابيس (الجدول والمسابقة)
        if db_quiz_id:
            # 🔥 [ الضربة القاضية ]: الحذف من جدول المشاركين (المحرك يقرأ من هنا في كل سؤال)
            supabase.table("quiz_participants").delete().eq("quiz_id", db_quiz_id).eq("chat_id", chat_id).execute()

            # (اختياري) تحديث السجل العام للمسابقة للمزامنة
            if is_public:
                res = supabase.table("active_quizzes").select("participants_ids").eq("id", db_quiz_id).execute()
                if res.data:
                    current_list = res.data[0].get('participants_ids', [])
                    if chat_id in current_list:
                        current_list.remove(chat_id)
                        supabase.table("active_quizzes").update({"participants_ids": current_list}).eq("id", db_quiz_id).execute()

        # 3. رد البوت الذكي
        bye_msgs = [
            "🚶 **تم الانسحاب بسلام..**\nنغادر الميدان الآن، شكراً لاستضافتكم!",
            "✅ **قُضي الأمر..**\nتم فك ارتباط المجموعة بالمسابقة الحالية بنجاح."
        ]
        await message.reply(random.choice(bye_msgs))

    except Exception as e:
        logging.error(f"❌ Error in withdrawal: {e}")
        await message.reply("⚠️ تم عزل المجموعة من الذاكرة المحلية، وجاري تنظيف السجلات السحابية.")
        
# ==========================================
# 2️⃣ المعالج الرئيسي للأوامر (عني، رتبتي، إلخ)
# ==========================================
@dp.message_handler(lambda m: m.text in ["عني", "رتبتي", "نقاطي", "محفظتي", "بروفايلي"])
@dp.message_handler(lambda m: m.reply_to_message and m.text in ["عنه", "رتبته", "نقاطه", "محفظته", "بروفايله"])
async def cmd_show_profile_global(message: types.Message):
    # 1. تحديد الهدف (أنا أو الشخص الذي تم الرد عليه)
    target = message.reply_to_message.from_user if message.reply_to_message else message.from_user
    uid = target.id

    # رسالة مؤقتة
    status = await message.reply("⏳ <b>جاري سحب البيانات من السجل العالمي...</b>", parse_mode="HTML")

    # 2. جلب البيانات من السجل (تأكد أن الدالة get_user_full_data تجلب الحقول الجديدة)
    user_data = await get_user_full_data(uid)
    
    if not user_data:
        await status.delete()
        msg = "❌ هذا المستخدم غير مسجل عالمياً." if message.reply_to_message else "❌ ليس لديك سجل عالمي بعد!"
        return await message.reply(msg)

    # 3. تنسيق البطاقة (التي عدلناها لتكون الألقاب والمقتنيات في أسطر)
    profile_text = await format_profile_card(user_data, uid)
    
    # 4. جلب الكيبورد (تم تمرير uid ليعمل المتجر لصاحب البروفايل)
    keyboard = get_profile_keyboard(uid) 
    
    # 5. محاولة جلب صورة البروفايل
    photo_id = None
    try:
        photos = await bot.get_user_profile_photos(uid, limit=1)
        if photos.total_count > 0:
            photo_id = photos.photos[0][-1].file_id
    except: 
        pass

    await status.delete() # حذف رسالة "جاري السحب"
    
    # 6. إرسال البروفايل (صورة أو نص)
    final_msg = None
    if photo_id:
        final_msg = await message.answer_photo(
            photo_id, 
            caption=profile_text, 
            parse_mode="HTML", 
            reply_markup=keyboard
        )
    else:
        final_msg = await message.answer(
            profile_text, 
            parse_mode="HTML", 
            reply_markup=keyboard
        )

    # 7. 🔥 نظام التدمير الذاتي (الحذف بعد دقيقة)
    await asyncio.sleep(60) # الانتظار لمدة 60 ثانية
    try:
        await final_msg.delete() # حذف البروفايل
        await message.delete()   # حذف أمر المستخدم لتنظيف المجموعة
    except:
        pass # في حال تم حذفها يدوياً من قبل مشرف
# ==========================================
# 6. معالج أمر البدء المطور في الخاص /start
# ==========================================
@dp.message_handler(commands=['start'], chat_type=types.ChatType.PRIVATE)
async def private_start_handler(message: types.Message):
    user_id = message.from_user.id
    first_name = message.from_user.first_name
    
    # لوحة الأزرار التفاعلية
    kb_start = InlineKeyboardMarkup(row_width=2)
    kb_start.add(
        InlineKeyboardButton("👤 ملفي الشخصي", callback_data=f"my_profile_{user_id}"),
        InlineKeyboardButton("🛒 المتجر الملكي", callback_data=f"open_shop_{user_id}")
    )
    kb_start.add(
        InlineKeyboardButton("👨‍💻 المطور: ياسر", url="https://t.me/Ya_79k"),
        InlineKeyboardButton("📢 قناة البوت", url="https://t.me/YourChannel")
    )
    kb_start.add(
        InlineKeyboardButton("➕ أضفني لمجموعتك الآن", url=f"https://t.me/{(await bot.get_me()).username}?startgroup=true")
    )

    welcome_msg = (
        f"👋 <b>أهلاً بك يا {first_name} في عالم التحدي!</b>\n\n"
        f"أنا بوت المسابقات الأكثر ذكاءً، استعد لاختبار معلوماتك.\n"
        f"      ❃┅┅┅┄┄┄┈•❃•┈┄┄┄┅┅┅❃\n"
        f"📜 <b>أوامر المستخدمين (في المجموعات او دردشة البوت):</b>\n"
        f"🔹 <code>تحكم</code> : لعرض لوحة التحكم يمكنك من هنا اعداد اقسامك وتجهيز المسابقات.\n"
        f"🔹 <code>مسابقة</code> : لبدء تشغيل مسابقتك بعد اعدادها او اعداد مسابقة .\n"
        f"🔸 <code>انسحاب او زدني توقف او زدني قف</code> : لايقاف المسابقة.\n"
        f"🔹 <code>ممتلكاتي </code> : لعرض ألقابك و مقتنياتك كلها.\n"
        f"🔹 <code>المتجر . متجر </code> : لعرض المتجر\n\n"
        f"🔹 <code>الاوامر </code> : لعرض الأوامر و اعدادات البوت.\n"
        f"🔹 <code>حسابي</code> : لعرض بطاقة حسابك البنكي بعد لعب مسابقة.\n" 
        f"🔹 <code>عني او رتبتي </code> : لعرض بطاقتك الشخصية ومقتنياتك.\n"
        f"🔹 <code>زدني </code> : لتجهيز مسابقة .\n"
        f"🔹 <code>تحويل</code> : او بالرد على رسالة المستخدم لتحويل فلوسك لمستخدم آخر.\n"
        f"🔹 <code>بالرد على رسالة المستخدم </code> : تحويل _ عنه _ حسابه _ ممتلكاته _ رتبته _ فلوسه. \n"
        f"🔹 <code>توب</code> : لرؤية الترتيب العالمي الأغنياء و الأذكياء و المجموعات .\n\n"
        f"      ❃┅┅┅┄┄┄┈•❃•┈┄┄┄┅┅┅❃\n"
        f"🛠️ <b>أوامر الإدارة (للمشرفين فقط ):</b>\n"
        f"🔸 <code>تفعيل</code> : لطلب المشاركة في المسابقات العامة\n"
        f"🔸 <code>مسابقة</code> :لبدء تشغيل المسابقة في نطاق مسابقة عامة.\n"
        f"🔸 <code>انسحاب</code> : لخروج من المسابقة.\n"
        f"🔸 <code>زدني قف </code> : لإيقاف المسابقة العامة لمن قام بتشغيلها او رتبته في البوت.\n"  
        f"      ❃┅┅┅┄┄┄┈•❃•┈┄┄┄┅┅┅❃\n"
        f"💎 <b>ميزة خاصة:</b> يمكنك جمع النقاط واستبدالها بجوائز داخل المتجر!  اكتب تحويل  لتحويل نقاطك الى لاعب اخر\n\n"
        f"💬 <b>للتواصل المباشر مع المطور (ياسر):</b>\n"
        f"اضغط هنا: @Ya_79k"
    )
    try:
        # Photo ID الخاص بصورة الترحيب (يفضل صورة فخمة للبوت)
        bot_photo = "AgACAgQAAxkBAA..." 
        await message.answer_photo(
            photo=bot_photo,
            caption=welcome_msg,
            reply_markup=kb_start,
            parse_mode="HTML"
        )
    except:
        await message.answer(welcome_msg, reply_markup=kb_start, parse_mode="HTML")
        
# ==========================================
# 5. الترحيب التلقائي بصورة البوت
# ==========================================
@dp.message_handler(content_types=types.ContentTypes.NEW_CHAT_MEMBERS)
async def welcome_bot_to_group(message: types.Message):
    for member in message.new_chat_members:
        if member.id == (await bot.get_me()).id:
            group_name = message.chat.title
            
            kb_welcome = InlineKeyboardMarkup(row_width=1)
            kb_welcome.add(
                InlineKeyboardButton("👑 مبرمج البوت (ياسر)", url="https://t.me/Ya_79k")
            )

            welcome_text = (
                f"👋 <b>أهلاً بكم في عالم المسابقات!</b>\n"
                f"تمت إضافتي بنجاح في: <b>{group_name}</b>\n"
                f"❃┅┅┅┄┄┄┈•❃•┈┄┄┄┅┅┅❃\n"
                f"🤖 <b>أنا بوت المسابقات الذكي (Questions Bot).</b>\n\n"
                f"🛠️ <b>كيفية البدء:</b>\n"
                f"يجب على المشرف كتابة أمر (تفعيل) لإرسال طلب للمطور.\n\n"
                f"📜 <b>الأوامر الأساسية:</b>\n"
                f"🔹 <b>تفعيل :</b> لطلب تشغيل البوت.\n"
                f"🔹 <b>تحكم :</b> لوحة الإعدادات (للمشرفين).\n"
                f"🔹 <b>مسابقة :</b> لبدء جولة أسئلة.\n"
                f"🔹 <b>عني :</b> لعرض ملفك الشخصي ونقاطك.\n"
                f"❃┅┅┅┄┄┄┈•❃•┈┄┄┄┅┅┅❃\n"
                f"📢 <i>اكتب (تفعيل) الآن لنبدأ الرحلة!</i>"
            )

            try:
                # ضع الـ File ID الذي حصلت عليه من @FileIdBot هنا
                bot_photo_id = "AgACAgQAAxkBAA..." # استبدل هذا بالكود الذي سيعطيك إياه البوت
                await message.answer_photo(
                    photo=bot_photo_id, 
                    caption=welcome_text, 
                    reply_markup=kb_welcome, 
                    parse_mode="HTML"
                )
            except:
                # في حال لم تضع الآيدي بعد أو حدث خطأ، يرسل نصاً فقط
                await message.answer(welcome_text, reply_markup=kb_welcome, parse_mode="HTML")

# ============================================================
# هاندلر عرض المقتنيات والنوادر (شخصي + للآخرين)
# الأوامر: مقتنياتي، ممتلكاتي، مقتنياته، ممتلكاته
# ============================================================
@dp.message_handler(lambda message: message.text and any(word in message.text for word in ["مقتنياتي", "ممتلكاتي", "مقتنياته", "ممتلكاته", "مقتنياتة", "ممتلكاتة"]))
async def show_user_assets(message: types.Message):
    # 1. تحديد الشخص المستهدف (أنا أم الشخص الذي رددت عليه؟)
    if any(word in message.text for word in ["مقتنياته", "ممتلكاته", "مقتنياتة", "ممتلكاتة"]):
        if message.reply_to_message:
            target_user = message.reply_to_message.from_user
        else:
            return await message.reply("⚠️ يجب أن ترد على رسالة العضو لعرض مقتنياته!")
    else:
        target_user = message.from_user

    status_msg = await message.reply("⏳ **جاري فتح الخزنة الملكية...**")

    try:
        # 2. جلب البيانات من الجدول الخاص بك
        res = supabase.table("users_global_profile").select("*").eq("user_id", target_user.id).execute()
        
        if not res.data:
            await status_msg.edit_text("❌ هذا العضو غير مسجل في سجلاتنا البنكية.")
            return

        p = res.data[0]
        
        # 3. معالجة بيانات JSON (titles و inventory)
        # بما أن الأعمدة في جدولك هي jsonb، سنتعامل معها كقوائم
        titles = p.get('titles') or []
        inventory = p.get('inventory') or []
        user_name = p.get('user_name', target_user.full_name)

        # 4. استخدام قالب التنسيق الفخم
        vault_text = format_vault_display(user_name, titles, inventory)

        # 5. عرض النتيجة مع زر إغلاق
        keyboard = InlineKeyboardMarkup()
        keyboard.add(InlineKeyboardButton("❌ إغلاق الخزنة", callback_data="close_card"))

        await status_msg.delete()
        await message.answer(vault_text, reply_markup=keyboard, parse_mode="HTML")

    except Exception as e:
        print(f"Error in Vault Handler: {e}")
        await status_msg.edit_text("⚠️ عذراً، حدث خطأ أثناء فحص المقتنيات.")

# ============================================================
# نظام التحويل البنكي المتطور - بنك زدني 2026
# الوظيفة: إدارة عمليات التحويل عبر الرد (Reply) أو رقم الحساب
# ============================================================

# 1. حالة التحويل بالرد (الطريقة السريعة)
@dp.message_handler(lambda message: message.text == "تحويل" and message.reply_to_message)
async def transfer_by_reply(message: types.Message, state: FSMContext):
    receiver = message.reply_to_message.from_user
    
    # حفظ بيانات المستلم في الذاكرة المؤقتة
    await state.update_data(target_id=receiver.id, target_name=receiver.full_name)
    
    # الانتقال لخطوة طلب المبلغ مباشرة
    await BankTransfer.waiting_for_amount.set()
    await message.reply(
        f"👤 <b>الـمـسـتـلـم:</b> {receiver.full_name}\n"
        f"💰 <b>أرسل الآن المبلغ المراد تحويله:</b>",
        parse_mode="HTML"
    )

# ------------------------------------------------------------
# 2. حالة التحويل عبر الأمر (طلب رقم الحساب)
@dp.message_handler(lambda message: message.text == "تحويل" and not message.reply_to_message)
async def transfer_by_acc(message: types.Message):
    # الانتقال لحالة انتظار رقم الحساب
    await BankTransfer.waiting_for_account.set()
    await message.reply(
        "🏦 <b>نظام التحويل البنكي</b>\n"
        "يرجى إرسال <b>رقم الحساب البنكي</b> للشخص المراد التحويل له:",
        parse_mode="HTML"
    )

# ------------------------------------------------------------

# 3. استقبال رقم الحساب والتحقق منه
@dp.message_handler(state=BankTransfer.waiting_for_account)
async def get_acc_num(message: types.Message, state: FSMContext):
    if not message.text.isdigit():
        return await message.reply("⚠️ يرجى إرسال رقم حساب صحيح (أرقام فقط).")
    
    # حفظ رقم الحساب والانتقال لطلب المبلغ
    await state.update_data(target_acc=int(message.text))
    await BankTransfer.waiting_for_amount.set()
    await message.reply("💰 <b>رقم الحساب صحيح.</b>\nالآن أرسل <b>المبلغ</b> المراد تحويله:")

# ------------------------------------------------------------

# 4. المرحلة النهائية: استقبال المبلغ وتنفيذ التحويل
@dp.message_handler(state=BankTransfer.waiting_for_amount)
async def finalize_transfer(message: types.Message, state: FSMContext):
    # التأكد أن المدخل رقم
    if not message.text.isdigit():
        return await message.reply("⚠️ يرجى إرسال مبلغ صحيح (أرقام فقط).")
    
    amount = int(message.text)
    if amount < 10: 
        return await message.reply("⚠️ الحد الأدنى للتحويل هو 10 ن.")
    
    # جلب البيانات المخزنة (سواء كان المستلم بالرد أو برقم الحساب)
    data = await state.get_data()
    sender_id = message.from_user.id
    
    # استدعاء دالة المعالجة التي تتواصل مع Supabase
    result_msg = await process_bank_transfer(
        sender_id=sender_id,
        amount=amount,
        receiver_id=data.get('target_id'),
        receiver_acc=data.get('target_acc')
    )
    
    # إرسال نتيجة العملية وإنهاء الحالة
    await message.answer(result_msg, parse_mode="HTML")
    await state.finish()

# ============================================================
# هاندلر فتح قائمة الدول عند الضغط على زر "إضافة دولتي"
# الوظيفة: استبدال لوحة البروفايل بلوحة اختيار الدول
# ============================================================
@dp.callback_query_handler(lambda c: c.data.startswith('set_country_'))
async def show_countries_list(callback_query: types.CallbackQuery):
    user_id = int(callback_query.data.split("_")[-1])
    
    # التأكد أن الشخص الذي ضغط هو صاحب البروفايل
    if callback_query.from_user.id != user_id:
        return await callback_query.answer("⚠️ هذه اللوحة ليست لك!", show_alert=True)

    await callback_query.message.edit_text(
        "🌍 **اختر دولتك من القائمة أدناه:**",
        reply_markup=get_countries_keyboard(user_id),
        parse_mode="Markdown"
    )
    await callback_query.answer()

# معالج زر "الرجوع للبروفايل"
@dp.callback_query_handler(lambda c: c.data.startswith('back_to_profile_'))
async def back_to_profile_handler(callback_query: types.CallbackQuery):
    user_id = int(callback_query.data.split("_")[-1])
    
    # هنا تعيد استدعاء دالة عرض البروفايل الأصلية الخاصة بك
    # سأفترض أن لديك دالة تجلب نص البروفايل
    profile_text = "🪪 **لوحة التحكم الخاصة بك**" 
    
    await callback_query.message.edit_text(
        profile_text,
        reply_markup=get_profile_keyboard(user_id),
        parse_mode="Markdown"
    )
    await callback_query.answer()
    
@dp.callback_query_handler(lambda c: c.data and c.data.startswith('sv_c_'))
async def process_save_country(callback_query: types.CallbackQuery):
    # استخدام split مع تحديد الأقسام لضمان الدقة
    data = callback_query.data.split('_')
    
    # التقسيم الصحيح بناءً على هيكلة sv_c_{name}_{flag}_{user_id}
    country_name = data[2]
    country_flag = data[3]
    user_id = int(data[4])

    # 1. التحديث في Supabase
    try:
        supabase.table("users_global_profile").update({
            "country_name": country_name,
            "country_flag": country_flag
        }).eq("user_id", user_id).execute()
        
        # 2. إشعار النجاح
        await callback_query.answer(f"تم تحديث دولتك إلى: {country_name} {country_flag} ✅", show_alert=False)
        
        # 3. تحديث رسالة البروفايل فوراً ليرى المستخدم التغيير
        res = supabase.table("users_global_profile").select("*").eq("user_id", user_id).single().execute()
        if res.data:
            from bot_file import format_profile_card, get_profile_keyboard # استيراد دوالك
            
            new_text = await format_profile_card(res.data, user_id)
            new_kb = get_profile_keyboard(user_id)
            
            await callback_query.message.edit_text(
                text=new_text,
                reply_markup=new_kb,
                parse_mode="HTML"
            )
            
    except Exception as e:
        print(f"Error: {e}")
        await callback_query.answer("⚠️ حدث خطأ أثناء الحفظ، حاول مجدداً.")
        
# ==========================================
# --- [ 4. محرك التنقل المنسق والمحمي ] ---
@dp.callback_query_handler(lambda c: c.data.startswith(('open_cat_', 'back_to_shop_', 'close_card_')), state="*")
async def shop_navigation_handler(call: types.CallbackQuery):
    data = call.data
    user_id = call.from_user.id
    
    # تقسيم البيانات بدقة
    # إذا كانت: open_cat_royal_123456
    # فالتقسيم سيكون: ['open', 'cat', 'royal', '123456']
    parts = data.split('_')
    owner_id = int(parts[-1]) # الأخير دائماً هو الآيدي

    # 🛡️ حارس البعسسة
    if user_id != owner_id:
        return await call.answer("🚫 : المتجر ليس لك!", show_alert=True)

    try:
        # 1. إغلاق المتجر
        if "close_card" in data:
            await call.message.delete()

        # 2. العودة للقائمة الرئيسية للمتجر
        elif "back_to_shop" in data:
            await call.message.edit_reply_markup(reply_markup=get_shop_main_keyboard(owner_id))
            await call.answer("🔙 : القائمة الرئيسية")

        # 3. فتح قسم (الملكية، البنات، إلخ)
        elif "open_cat_" in data:
            # نأخذ العضو الثالث في المصفوفة وهو اسم القسم
            category = parts[2] 
            
            # استدعاء دالة المنتجات (تأكد أنها تقبل متغيرين: القسم والآيدي)
            kb = get_products_keyboard(category, owner_id)
            await call.message.edit_reply_markup(reply_markup=kb)
            await call.answer(f"📂 : قسم {category}")

    except Exception as e:
        import logging
        logging.error(f"Shop Error: {e}")
        # إذا حصل خطأ، سنطبع السبب الحقيقي في الكونسول لنعرفه
        await call.answer(f"❌ : خطأ برمي: {e}")
# ==========================================
# ................. الشراء....................
# ==========================================
@dp.callback_query_handler(lambda c: c.data.startswith('buy_'), state="*")
async def handle_purchase_confirmation(call: types.CallbackQuery):
    user_id = call.from_user.id
    parts = call.data.split('_')
    item_id, category, owner_id = parts[1], parts[2], int(parts[3])

    if user_id != owner_id:
        return await call.answer("🚫 : المتجر ليس لك!", show_alert=True)

    product = ITEMS_DB.get(category, {}).get(item_id)
    if not product: return await call.answer("⚠️ : المنتج غير متوفر!")

    item_name = product['name']
    price = product['price']

    # كيبورد التأكيد (نعم و تراجع فقط)
    confirm_kb = InlineKeyboardMarkup(row_width=2)
    confirm_kb.add(
        InlineKeyboardButton("✅ نعم، شراء", callback_data=f"confbuy_{item_id}_{category}_{user_id}"),
        InlineKeyboardButton("🔙 تراجع", callback_data=f"open_cat_{category}_{user_id}")
    )

    confirm_text = (
        f"<b>🛒 تأكيد عملية الشراء</b>\n"
        f"  — — — — — — — — — —\n"
        f"📦 السلعة: <b>{item_name}</b>\n"
        f"💰 الثمن: <code>{price}</code> ن\n"
        f"  — — — — — — — — — —\n"
        f"⚠️ هل أنت متأكد من رغبتك في الشراء؟"
    )

    await call.message.edit_text(confirm_text, reply_markup=confirm_kb, parse_mode="HTML")
        
# --- [ معالج التنفيذ الفعلي بعد الضغط على نعم ] ---
@dp.callback_query_handler(lambda c: c.data.startswith('confbuy_'), state="*")
async def execute_actual_purchase(call: types.CallbackQuery):
    user_id = call.from_user.id
    parts = call.data.split('_')
    item_id, category, owner_id = parts[1], parts[2], int(parts[3])

    product = ITEMS_DB.get(category, {}).get(item_id)
    price = product['price']
    item_name = product['name']

    # جلب بيانات المستخدم
    res = supabase.table("users_global_profile").select("*").eq("user_id", user_id).execute()
    user_data = res.data[0]
    wallet = user_data.get('wallet', 0)

    if wallet < price:
        return await call.answer(f"❌ رصيدك {wallet}ن لا يكفي!", show_alert=True)

    # تجهيز المخازن
    current_titles = user_data.get('titles') or []
    current_inventory = user_data.get('inventory') or []
    current_cards = user_data.get('cards_inventory') or {}

    update_payload = {"wallet": wallet - price}

    # --- [ منطق التوزيع الذكي ] ---
    if category == "cards":
        # إضافة الكروت لـ cards_inventory بالعدد
        current_cards[item_id] = current_cards.get(item_id, 0) + 1
        update_payload["cards_inventory"] = current_cards
    
    elif category in ["gifts", "rare", "estates"]:
        # إضافة المقتنيات لـ inventory
        if item_name in current_inventory:
            return await call.answer(f"📦 تملك {item_name} مسبقاً!", show_alert=True)
        current_inventory.append(item_name)
        update_payload["inventory"] = current_inventory
        
    else: # الألقاب (royal, girls)
        # إضافة الألقاب لـ titles
        if item_name in current_titles:
            return await call.answer(f"👑 تملك لقب {item_name} مسبقاً!", show_alert=True)
        current_titles.append(item_name)
        update_payload["titles"] = current_titles

    # حفظ في سوبابيس
    supabase.table("users_global_profile").update(update_payload).eq("user_id", user_id).execute()

    # رسالة طائرة في نصف الشاشة
    await call.answer(f"🎉 مبروك! اشتريت {item_name}\nرصيدك المتبقي: {wallet-price}ن", show_alert=True)

    # العودة للمتجر الرئيسي
    new_text = f"✨ <b>تمت العملية بنجاح!</b>\nمحفظتك الآن: <code>{wallet-price}</code> ن"
    await call.message.edit_text(new_text, reply_markup=get_shop_main_keyboard(user_id), parse_mode="HTML")
    
# ============================================================
# هاندلر استدعاء لوحة المطور (لوحتي، المطور، غرفة العمليات)
# ============================================================
@dp.message_handler(lambda message: message.text in ['لوحتي', 'المطور', 'غرفتي', 'غرفة العمليات', 'الإدارة'], chat_type=types.ChatType.PRIVATE)
async def admin_dashboard_trigger(message: types.Message):
    """
    استدعاء لوحة التحكم الخاصة بالمطور مع التحقق من الهوية (مثل نظام التفعيل)
    """
    user_id = message.from_user.id

    # التحقق الصارم: هل المستخدم هو المطور؟
    if user_id != ADMIN_ID:
        return await message.reply("⚠️ <b>عذراً، هذه اللوحة مخصصة لمطور النظام فقط.</b>", parse_mode="HTML")

    try:
        # 1. جلب البيانات الحية من سوبابيس (إحصائيات المجموعات)
        res = supabase.table("groups_hub").select("*").execute()
        
        # تصنيف البيانات لجعل اللوحة "حية"
        active = len([g for g in res.data if g['status'] == 'active'])
        blocked = len([g for g in res.data if g['status'] == 'blocked'])
        pending = len([g for g in res.data if g['status'] == 'pending'])
        total_points = sum([g.get('total_group_score', 0) for g in res.data])

        # 2. تصميم النص الفخم لغرفة العمليات (نفس أسلوب لوحة التحكم)
        txt = (
            "👑 <b>غرفة العمليات الرئيسية</b>\n"
            "━━━━━━━━━━━━━━\n"
            f"✅ المجموعات النشطة : <b>{active}</b>\n"
            f"🚫 المجموعات المحظورة : <b>{blocked}</b>\n"
            f"⏳ طلبات معلقة : <b>{pending}</b>\n"
            f"🏆 إجمالي نقاط الهب : <b>{total_points:,}</b>\n"
            "━━━━━━━━━━━━━━\n"
            "👇 <b>أهلاً بك يا مطور، اختر قسماً لإدارته :</b>"
        )
        
        # 3. إرسال اللوحة مع الكيبورد الرئيسي الخاص بك
        await message.answer(
            txt, 
            reply_markup=get_main_admin_kb(), 
            parse_mode="HTML"
        )

    except Exception as e:
        logging.error(f"Admin Dashboard Error: {e}")
        await message.answer("❌ <b>حدث خطأ أثناء الاتصال بقاعدة البيانات الموحدة.</b>", parse_mode="HTML")

# =========================================
# 6. أمر التفعيل (Request Activation)
# =========================================
@dp.message_handler(lambda m: m.text == "تفعيل", chat_type=[types.ChatType.GROUP, types.ChatType.SUPERGROUP])
async def activate_group_hub(message: types.Message):
    user_id = message.from_user.id
    chat_member = await message.chat.get_member(user_id)
    
    if not (chat_member.is_chat_admin() or user_id == ADMIN_ID):
        return await message.reply("⚠️ هذا الأمر مخصص لمشرفي القروب فقط.")

    group_id = message.chat.id
    group_name = message.chat.title

    try:
        res = supabase.table("groups_hub").select("*").eq("group_id", group_id).execute()
        
        if res.data:
            status = res.data[0]['status']
            if status == 'active':
                return await message.reply("🛡️ القروب مفعل مسبقاً وجاهز للعمل!", parse_mode="HTML")
            elif status == 'pending':
                return await message.reply("⏳ طلبكم قيد المراجعة، انتظر موافقة المطور.", parse_mode="HTML")
            elif status == 'blocked':
                return await message.reply("🚫 هذا القروب محظور من قبل المطور.", parse_mode="HTML")
        
        # إدخال القروب في pending
        supabase.table("groups_hub").insert({
            "group_id": group_id,
            "group_name": group_name,
            "status": "pending",
            "total_group_score": 0
        }).execute()

        # إشعار المطور
        kb_fast_action = InlineKeyboardMarkup(row_width=2).add(
            InlineKeyboardButton("✅ موافقة", callback_data=f"auth_approve_{group_id}"),
            InlineKeyboardButton("🚫 رفض وحظر", callback_data=f"auth_block_{group_id}")
        )
        await bot.send_message(ADMIN_ID, 
            f"🔔 طلب تفعيل جديد!\n"
            f"👥 القروب: {group_name}\n"
            f"🆔 {group_id}\n"
            f"اتخذ قرارك الآن:", 
            reply_markup=kb_fast_action, 
            parse_mode="HTML")

        # إشعار القروب
        await message.reply("✅ تم إرسال طلب التفعيل، انتظر موافقة المطور.", parse_mode="HTML")

    except Exception as e:
        logging.error(f"Activation Error: {e}")
        await message.reply("❌ حدث خطأ تقني في قاعدة البيانات.")

# ==========================================
# 2. تعديل أمر "تحكم" لضمان عدم العمل إلا بعد التفعيل
# ==========================================
@dp.message_handler(lambda m: m.text == "تحكم")
async def control_panel(message: types.Message):
    user_id = message.from_user.id
    group_id = message.chat.id

    # في المجموعات، نتحقق من حالة التفعيل
    if message.chat.type != 'private':
        # إذا لم يكن المطور، نتحقق من حالة القروب
        if user_id != ADMIN_ID:
            status = await get_group_status(group_id)
            if status != "active":
                return await message.reply("⚠️ <b>هذا القروب غير مفعل.</b>\nيجب أن يوافق المطور على طلب التفعيل أولاً.", parse_mode="HTML")
            
            # فحص هل المستخدم مشرف
            member = await bot.get_chat_member(group_id, user_id)
            if not (member.is_chat_admin() or member.is_chat_creator()):
                return await message.reply("⚠️ لوحة التحكم مخصصة للمشرفين فقط.")

    # إذا كان المطور أو قروب مفعل، تظهر اللوحة
    txt = (f"👋 أهلاً بك في لوحة الإعدادات\n"
           f"👑 المطور: <b>{OWNER_USERNAME}</b>")
    
    await message.answer(txt, reply_markup=get_main_control_kb(user_id), parse_mode="HTML")

# ============================================================
# هاندلر استدعاء المتجر (المتجر، متجر، أوامر المتجر)
# ============================================================
@dp.message_handler(lambda message: message.text in ['المتجر', 'متجر'] or message.text.startswith('/shop'))
async def cmd_open_shop_bazaar(message: types.Message):
    """
    استدعاء متجر البوت مع عرض الرصيد والحذف التلقائي
    """
    user_id = message.from_user.id
    
    try:
        # 1. جلب بيانات المستخدم (المحفظة) من سوبابيس
        res = supabase.table("users_global_profile").select("wallet").eq("user_id", user_id).execute()
        
        if not res.data:
            return await message.reply("⚠️ يجب أن يكون لديك حساب مسجل لفتح المتجر.")
        
        wallet = res.data[0].get('wallet', 0)
        
        # 2. تجهيز النص الفخم (استدعاء دالة التنسيق الخاصة بك)
        shop_text = await format_shop_bazaar_card(wallet)
        
        # 3. إرسال المتجر مع كيبورد الأقسام وحماية صاحب الطلب (owner_id)
        sent_shop = await message.reply(
            shop_text,
            reply_markup=get_shop_main_keyboard(user_id), # تمرير الـ ID للحماية
            parse_mode="HTML"
        )
        
        # 4. ميزة التطهير التلقائي (حذف المتجر بعد 60 ثانية لتقليل الزحام)
        await asyncio.sleep(60)
        try:
            await sent_shop.delete()
            await message.delete() # حذف كلمة "متجر" التي أرسلها المستخدم
        except:
            pass

    except Exception as e:
        logging.error(f"Shop Error: {e}")
        await message.reply("❌ المتجر مغلق حالياً للصيانة، حاول لاحقاً.")

# ============================================================
# معالج العودة للمتجر (Callback)
# ============================================================
@dp.callback_query_handler(lambda c: c.data.startswith("back_to_shop_"), state="*")
async def back_to_shop_handler(c: types.CallbackQuery):
    owner_id = int(c.data.split("_")[-1])
    
    # حماية: المشتري فقط من يمكنه التحكم
    if c.from_user.id != owner_id:
        return await c.answer("⚠️ هذا المتجر ليس لك! اطلب متجرك الخاص بكتابة 'متجر'.", show_alert=True)

    # تحديث المحفظة قبل العودة
    res = supabase.table("users_global_profile").select("wallet").eq("user_id", owner_id).execute()
    wallet = res.data[0].get('wallet', 0)
    
    shop_text = await format_shop_bazaar_card(wallet)
    
    await c.message.edit_text(
        shop_text,
        reply_markup=get_shop_main_keyboard(owner_id),
        parse_mode="HTML"
    )
    await c.answer("🔙 عدنا لساحة ")
    
# التعديل في السطر 330 (أضفنا close_bot_)
@dp.callback_query_handler(lambda c: c.data.startswith(('custom_add_', 'dev_', 'setup_quiz_', 'dev_leaderboard_', 'close_bot_', 'back_', 'open_shop_')), state="*")
async def handle_control_buttons(c: types.CallbackQuery, state: FSMContext):
    data_parts = c.data.split('_')
    action = data_parts[0] 
    owner_id = int(data_parts[-1])

    # 🛑 [ الأمان ]
    if c.from_user.id != owner_id:
        return await c.answer("⚠️ لا تلمس أزرار غيرك! 😂", show_alert=True)

    # 1️⃣ [ زر الإغلاق ] - فحص الكلمة بالكامل أو أول جزء
    if action == "close":
        await c.answer("تم إغلاق اللوحة ✅")
        return await c.message.delete()

    # 2️⃣ [ زر الرجوع ] - النسخة المصلحة (التعديل بدل الإرسال)
    elif action == "back":
        await state.finish()
        await c.answer("🔙 جاري العودة...")
        # بدلاً من استدعاء control_panel التي ترسل رسالة جديدة، نعدل الرسالة الحالية
        return await c.message.edit_text(
            f"👋 **أهلاً بك في لوحة التحكم الرئيسية**\n\nاختر من الأسفل ما تود القيام به:",
            reply_markup=get_main_control_kb(owner_id), # تأكد من وضع دالة الكيبورد الرئيسي هنا
            parse_mode="Markdown"
        )

    # 3️⃣ [ زر إضافة خاصة ]
    elif action == "custom":
        await c.answer()
        # التعديل هنا: يجب أن يكون السطر القادم تحت elif مباشرة (4 مسافات)
        return await custom_add_menu(c, state=state)

    # 4️⃣ [ زر تجهيز المسابقة ]
    elif action == "setup":
        await c.answer()
        keyboard = get_setup_quiz_kb(owner_id)
        return await c.message.edit_text(
            "🏆 **مرحباً بك في معمل تجهيز المسابقات!**\n\nمن أين تريد جلب الأسئلة لمسابقتك؟",
            reply_markup=keyboard,
            parse_mode="Markdown"
        )
        # 5️⃣ [ محرك فتح المتجر العالمي ] 🛒
    elif action == "open" and "shop" in data_parts:
        await c.answer("💰 جاري فتح المتجر الملكي...")
        
        # 1. جلب رصيد المستخدم من سوبابيس (أو وضعه 0 كاحتياط)
        try:
            res = supabase.table("users_global_profile").select("wallet").eq("user_id", owner_id).execute()
            wallet = res.data[0]['wallet'] if res.data and len(res.data) > 0 else 0
        except Exception as e:
            print(f"Error fetching wallet: {e}")
            wallet = 0 
            
        # 2. تجهيز النص الفخم (تأكد من وجود دالة format_shop_bazaar_card)
        shop_text = await format_shop_bazaar_card(wallet)
        
        # 3. تحديث الكيبورد واستدعاء دالة الأقسام
        # أضفنا owner_id لكي تمر الحماية للأزرار التالية
        return await c.message.edit_text(
            shop_text,
            reply_markup=get_shop_main_keyboard(owner_id), 
            parse_mode="HTML"
        )
        # 6️⃣ [ محرك فتح لوحة الصدارة العالمية ] 🏆
    elif action == "dev" and "leaderboard" in data_parts:
        await c.answer("🏆 جاري فتح سجلات الشرف...")
        
        try:
            # 1. استدعاء الدالة التي تجهز النص الفخم والكيبورد
            # (تأكد من وجود الدالة التي صممناها سابقاً في ملفك)
            leaderboard_text, leaderboard_kb = get_leaderboard_main_message()
            
            # 2. تحديث الرسالة الحالية بلوحة الصدارة
            await c.message.edit_text(
                text=leaderboard_text,
                reply_markup=leaderboard_kb,
                parse_mode="HTML"
            )
            
        except Exception as e:
            print(f"Error opening leaderboard: {e}")
            await c.answer("⚠️ عذراً، تعذر فتح لوحة الصدارة حالياً.", show_alert=True)
            
# --- [ 4. محرك التنقل بين أقسام المتجر ] ---
@dp.callback_query_handler(lambda c: c.data.startswith('open_cat_') or c.data in ['back_to_shop', 'close_card'])
async def shop_navigation_handler(call: types.CallbackQuery):
    user_id = call.from_user.id
    data = call.data

    # 🛡️ حارس البعسسة: التأكد أن الضاغط هو صاحب الطلب
    if call.message.reply_to_message and call.message.reply_to_message.from_user.id != user_id:
        return await call.answer("🚫 : المتجر ليس لك يا شريك! اطلب /متجر خاص بك.", show_alert=True)

    try:
        # أ. إغلاق المتجر
        if data == "close_card":
            await call.message.delete()
            await call.answer("✅ : تم إغلاق المتجر")

        # ب. العودة للقائمة الرئيسية
        elif data == "back_to_shop":
            await call.message.edit_reply_markup(reply_markup=get_shop_main_keyboard())
            await call.answer("🔙 : العودة للقائمة الرئيسية")

        # ج. فتح قسم محدد (الملكية، البنات، إلخ)
        elif data.startswith("open_cat_"):
            category = data.replace("open_cat_", "")
            
            # فحص إذا كان القسم موجوداً في مصفوفتنا ITEMS_DB
            if category in ITEMS_DB:
                await call.message.edit_reply_markup(reply_markup=get_products_keyboard(category))
                await call.answer(f"📂 : تم فتح قسم {category}")
            elif category == "cards":
                # قسم الكروت سنبرمجه لاحقاً كخطوة مستقلة
                await call.answer("🃏 : قسم الكروت قيد التجهيز في الخطوة القادمة!", show_alert=True)
            else:
                await call.answer("⚠️ : هذا القسم غير متوفر حالياً")

    except Exception as e:
        import logging
        logging.error(f"Error in Shop Navigation: {e}")
        await call.answer("❌ : حدث خطأ أثناء التنقل!")
        
# --- معالج أزرار التفعيل (الإصدار الآمن والمضمون) ---
@dp.callback_query_handler(lambda c: c.data.startswith(('auth_approve_', 'auth_block_')), user_id=ADMIN_ID)
async def process_auth_callback(c: types.CallbackQuery):
    action = c.data.split('_')[1]
    target_id = int(c.data.split('_')[2])
    
    if action == "approve":
        supabase.table("groups_hub").update({"status": "active"}).eq("group_id", target_id).execute()
        await c.answer("تم التفعيل ✅", show_alert=True)
        await bot.send_message(target_id, "🎉 مبارك! القروب مفعل. أرسل كلمة (مسابقة) للبدء.")
        
    elif action == "block":
        supabase.table("groups_hub").update({"status": "blocked"}).eq("group_id", target_id).execute()
        await c.answer("تم الحظر ❌", show_alert=True)
        await bot.send_message(target_id, "🚫 تم رفض طلب التفعيل وحظر القروب.")
    
    await c.message.delete()
    await admin_manage_groups(c)
    

# --- [ 2. إدارة الأقسام والأسئلة (النسخة النهائية المصلحة) ] ---
@dp.callback_query_handler(lambda c: c.data.startswith('custom_add'), state="*")
async def custom_add_menu(c: types.CallbackQuery, state: FSMContext = None):
    if state:
        await state.finish()
    
    data_parts = c.data.split('_')
    try:
        owner_id = int(data_parts[-1])
    except (ValueError, IndexError):
        owner_id = c.from_user.id

    if c.from_user.id != owner_id:
        return await c.answer("⚠️ هذي اللوحة مش حقك! 😂", show_alert=True)

    kb = get_categories_kb(owner_id)

    # هنا نستخدم edit_text لضمان التعديل بدل الإرسال الجديد
    await c.message.edit_text(
        "⚙️ **لوحة إعدادات أقسامك الخاصة:**\n\nاختر من القائمة أدناه لإدارة أقسامك وأسئلتك:", 
        reply_markup=kb, 
        parse_mode="Markdown"
    )
    await c.answer()

@dp.callback_query_handler(lambda c: c.data.startswith('back_to_main'), state="*")
async def back_to_main_panel(c: types.CallbackQuery, state: FSMContext = None):
    if state:
        await state.finish()
    
    # استخراج البيانات اللازمة
    owner_id = int(c.data.split('_')[-1])
    first_name = c.from_user.first_name
    
    # استدعاء كيبورد لوحة التحكم الرئيسية
    kb = get_main_control_kb(owner_id)

    # القالب الفخم الجديد بدون المعرف وبالتنسيق الملكي
    main_text = (
        f"👋 <b>أهلاً بك يا {first_name} في عالم التحدي!</b>\n\n"
        f"أنا بوت المسابقات الأكثر ذكاءً، استعد لاختبار معلوماتك.\n"
        f"      ❃┅┅┅┄┄┄┈•❃•┈┄┄┄┅┅┅❃\n"
        f"📜 <b>إليك شرح عمل الأزرار في الأسفل:</b>\n"
        f"🔹 <code>📝 إضافة خاصة</code> : يمكنك من هنا إنشاء وكتابة أقسام تحتوي على أسئلتك.\n"
        f"🔹 <code>📅 جلسة سابقة</code> : يمكنك من هنا إكمال إعداد قسم أسئلتك.\n"
        f"🔹 <code>🏆 تجهيز مسابقة</code> : من هنا يمكنك تجهيز واختيار الأسئلة المراد تشغيلها واختيار مصدر الأسئلة."
    )

    # التعديل الجوهري باستخدام HTML
    try:
        await c.message.edit_text(
            text=main_text,
            reply_markup=kb,
            parse_mode="HTML"
        )
    except Exception as e:
        # في حال كانت الرسالة هي نفسها لتجنب خطأ "Message is not modified"
        pass

    await c.answer("🔙 تمت العودة للقائمة الرئيسية")

@dp.callback_query_handler(lambda c: c.data.startswith('add_new_cat'), state="*")
async def btn_add_cat(c: types.CallbackQuery):
    owner_id = int(c.data.split('_')[-1])
    if c.from_user.id != owner_id:
        return await c.answer("⚠️ لا يمكنك الإضافة في لوحة غيرك!", show_alert=True)

    await c.answer() 
    await Form.waiting_for_cat_name.set()
    
    kb = InlineKeyboardMarkup().add(
        InlineKeyboardButton("🔙 إلغاء والعودة", callback_data=f"custom_add_{owner_id}")
    )
    # تحديث الرسالة لطلب الاسم لمنع التراكم
    await c.message.edit_text("📝 **اكتب اسم القسم الجديد الآن:**", reply_markup=kb, parse_mode="Markdown")

@dp.message_handler(state=Form.waiting_for_cat_name)
async def save_cat(message: types.Message, state: FSMContext):
    cat_name = message.text.strip()
    user_id = message.from_user.id
    
    try:
        supabase.table("categories").insert({
            "name": cat_name, 
            "created_by": str(user_id)
        }).execute()
        
        await state.finish()
        
        # عند النجاح، نرسل رسالة جديدة كإشعار ثم نعطيه زر العودة الذي يقوم بالتعديل
        kb = InlineKeyboardMarkup().add(
            InlineKeyboardButton("🔙 العودة للأقسام", callback_data=f"custom_add_{user_id}")
        )
        await message.answer(f"✅ تم حفظ القسم **'{cat_name}'** بنجاح.", reply_markup=kb, parse_mode="Markdown")

    except Exception as e:
        await state.finish()
        kb = InlineKeyboardMarkup().add(InlineKeyboardButton("⬅️ الرجوع", callback_data=f"custom_add_{user_id}"))
        await message.answer("⚠️ حدث خطأ أو الاسم مكرر. حاول مرة أخرى.", reply_markup=kb)

# --- 1. نافذة إعدادات القسم (عند الضغط على اسمه) ---
@dp.callback_query_handler(lambda c: c.data.startswith('manage_questions_'))
async def manage_questions_window(c: types.CallbackQuery):
    # تفكيك البيانات: manage_questions_ID_USERID
    data = c.data.split('_')
    cat_id = data[2]
    owner_id = int(data[3])

    # حماية من المبعسسين
    if c.from_user.id != owner_id:
        return await c.answer("⚠️ هذه اللوحة ليست لك!", show_alert=True)

    await c.answer()
    # استدعاء الدالة الموحدة
    await show_category_settings_ui(c.message, cat_id, owner_id, is_edit=True)


# --- 2. بدء تعديل اسم القسم ---
@dp.callback_query_handler(lambda c: c.data.startswith('edit_cat_'))
async def edit_category_start(c: types.CallbackQuery, state: FSMContext):
    data = c.data.split('_')
    cat_id = data[2]
    owner_id = int(data[3])

    if c.from_user.id != owner_id:
        return await c.answer("⚠️ لا تملك صلاحية التعديل!", show_alert=True)

    await c.answer()
    await state.update_data(edit_cat_id=cat_id, edit_owner_id=owner_id)
    await Form.waiting_for_new_cat_name.set()
    
    # زر تراجع ذكي يعود لصفحة الإعدادات
    kb = InlineKeyboardMarkup().add(
        InlineKeyboardButton("🚫 تراجع", callback_data=f"manage_questions_{cat_id}_{owner_id}")
    )
    await c.message.edit_text("📝 **نظام التعديل:**\n\nأرسل الآن الاسم الجديد للقسم:", reply_markup=kb)

# --- 3. حفظ الاسم الجديد (استدعاء الدالة الموحدة بعد الحفظ) ---
@dp.message_handler(state=Form.waiting_for_new_cat_name)
async def save_edited_category(message: types.Message, state: FSMContext):
    data = await state.get_data()
    cat_id = data['edit_cat_id']
    owner_id = data['edit_owner_id']
    new_name = message.text.strip()
    
    # تحديث الاسم في Supabase
    supabase.table("categories").update({"name": new_name}).eq("id", cat_id).execute()
    
    # تنظيف الشات
    try: await message.delete()
    except: pass

    await state.finish()
    
    # الاستدعاء الذكي: نرسل رسالة جديدة (is_edit=False) لأننا حذفنا رسالة المستخدم
    # ونعرض لوحة الإعدادات بالاسم الجديد فوراً
    await show_category_settings_ui(message, cat_id, owner_id, is_edit=False)
# ==========================================
# --- 3. نظام إضافة سؤال (محمي ومنظم) ---
# ==========================================
# 1️⃣ [بداية العملية]: فتح محضر إضافة سؤال جديد
@dp.callback_query_handler(lambda c: c.data.startswith('add_q_'))
async def start_add_question(c: types.CallbackQuery, state: FSMContext):
    data_parts = c.data.split('_')
    cat_id, owner_id = data_parts[2], int(data_parts[3])

    if c.from_user.id != owner_id:
        return await c.answer("⚠️ عذراً.. لا تملك صلاحيات 'رئيس اللجنة' لتعديل هذا القسم!", show_alert=True)

    await c.answer("جاري فتح السجل الأكاديمي..")
    await state.update_data(current_cat_id=cat_id, current_owner_id=owner_id, last_bot_msg_id=c.message.message_id)
    await Form.waiting_for_question.set()
    
    # زر إغلاق اللجنة (الإلغاء) المصلح
    kb = InlineKeyboardMarkup().add(
        InlineKeyboardButton("🚫 إغلاق اللجنة (إلغاء)", callback_data=f"manage_questions_{cat_id}_{owner_id}")
    )
    await c.message.edit_text("📝 **محضر إضافة سؤال جديد:**\n\nتفضل ، اكتب الآن نص السؤال التعليمي:", reply_markup=kb)

# 2️⃣ [استلام السؤال]: توثيق نص السؤال والانتقال للإجابة
@dp.message_handler(state=Form.waiting_for_question)
async def process_q_text(message: types.Message, state: FSMContext):
    data = await state.get_data()
    await state.update_data(q_content=message.text)
    
    try:
        await message.delete()
        await bot.delete_message(message.chat.id, data['last_bot_msg_id'])
    except: pass

    await Form.waiting_for_ans1.set()
    msg = await message.answer("✅ تم تدوين نص السؤال..\n\nالآن أرسل **الإجابة الصحيحة** والوحيدة لاعتمادها:")
    await state.update_data(last_bot_msg_id=msg.message_id)

# 3️⃣ [الحفظ النهائي]: التوثيق مع فحص التكرار الاستباقي
@dp.message_handler(state=Form.waiting_for_ans1)
async def process_first_ans(message: types.Message, state: FSMContext):
    data = await state.get_data()
    cat_id = data.get('current_cat_id')
    owner_id = data.get('current_owner_id')
    q_content = data.get('q_content').strip() # إزالة المسافات الزائدة لزيادة دقة الفحص
    ans_text = message.text.strip()

    try:
        # 🔍 [ فحص الأرشيف الأكاديمي ] - هل السؤال موجود مسبقاً في هذا القسم؟
        check_dup = supabase.table("questions").select("id").eq("category_id", cat_id).eq("question_content", q_content).execute()
        
        if check_dup.data:
            await message.answer(f"⚠️ **تنبيه أكاديمي:**\nعذراً  هذا السؤال موجود مسبقاً في هذا القسم! لا يمكن تكرار البيانات في السجل.")
            await state.finish()
            # العودة للوحة الإعدادات فوراً لمنع التكرار
            return await show_category_settings_ui(message, cat_id, owner_id, is_edit=False)

        # 💾 [ إذا لم يوجد تكرار ] -> تنفيذ عملية الحفظ
        supabase.table("questions").insert({
            "category_id": cat_id,
            "question_content": q_content,
            "correct_answer": ans_text,
            "created_by": str(owner_id)
        }).execute()

        await message.answer(f"✅ **تم تسجيل الاجابة لا تحتاج إلى اضافة اجابة دع الباقي علينا!**\nالسؤال: ({q_content})\nالإجابة: ({ans_text})\n\nتمت إضافته للسجل الأكاديمي. 🎓")
        
        # إنهاء الحالة (FSM) وتنظيف الشاشة
        await state.finish()
        try:
            await message.delete()
            await bot.delete_message(message.chat.id, data['last_bot_msg_id'])
        except: pass
        
        # العودة للوحة الإعدادات الرئيسية
        await show_category_settings_ui(message, cat_id, owner_id, is_edit=False)
        
    except Exception as e:
        logging.error(f"Error in Academic Check/Save: {e}")
        await message.answer("🚨 عذراً ، حدث خلل فني غير متوقع في قاعدة البيانات.")
        await state.finish()
        
# ==========================================
# --- 5. نظام عرض الأسئلة (المحمي بآيدي صاحب القسم) ---
# ==========================================
@dp.callback_query_handler(lambda c: c.data.startswith('view_qs_'), state="*")
async def view_questions(c: types.CallbackQuery):
    # تفكيك البيانات: view_qs_CATID_OWNERID
    data = c.data.split('_')
    cat_id = data[2]
    owner_id = int(data[3])

    # 🛑 حماية من المبعسسين
    if c.from_user.id != owner_id:
        return await c.answer("⚠️ لا يمكنك عرض أسئلة في لوحة غيرك!", show_alert=True)

    await c.answer()

    # جلب الأسئلة من Supabase
    questions = supabase.table("questions").select("*").eq("category_id", cat_id).execute()
    
    # إذا كان القسم فارغاً
    if not questions.data:
        kb = InlineKeyboardMarkup().add(
            InlineKeyboardButton("🔙 رجوع", callback_data=f"manage_questions_{cat_id}_{owner_id}")
        )
        return await c.message.edit_text("⚠️ لا توجد أسئلة مضافة في هذا القسم حالياً.", reply_markup=kb)

    # بناء نص عرض الأسئلة
    txt = f"🔍 قائمة الأسئلة المضافة:\n"
    txt += "--- --- --- ---\n\n"
    
    for i, q in enumerate(questions.data, 1):
        txt += f"<b>{i} - {q['question_content']}</b>\n"
        txt += f"✅ ج1: {q['correct_answer']}\n"
        # التحقق من وجود إجابة بديلة (ج2)
        if q.get('alternative_answer'):
            txt += f"💡 ج2: {q['alternative_answer']}\n"
        txt += "--- --- --- ---\n"

    # أزرار التحكم في القائمة (محمية بالآيدي)
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("🗑️ حذف الأسئلة", callback_data=f"del_qs_menu_{cat_id}_{owner_id}"),
        InlineKeyboardButton("🔙 رجوع لإعدادات القسم", callback_data=f"manage_questions_{cat_id}_{owner_id}")
    )
    
    # استخدام HTML ليكون النص أوضح (bold للعناوين)
    await c.message.edit_text(txt, reply_markup=kb, parse_mode="HTML")

# --- 6. نظام حذف الأسئلة (المحمي) ---

@dp.callback_query_handler(lambda c: c.data.startswith('del_qs_menu_'))
async def delete_questions_menu(c: types.CallbackQuery):
    data = c.data.split('_')
    # del(0) _ qs(1) _ menu(2) _ catid(3) _ ownerid(4)
    cat_id = data[3]
    owner_id = int(data[4])

    if c.from_user.id != owner_id:
        return await c.answer("⚠️ لا تملك صلاحية الحذف هنا!", show_alert=True)

    await c.answer()
    res = supabase.table("questions").select("*").eq("category_id", cat_id).execute()
    questions = res.data
    
    kb = InlineKeyboardMarkup(row_width=1)
    if questions:
        for q in questions:
            kb.add(InlineKeyboardButton(
                f"🗑️ حذف: {q['question_content'][:25]}...", 
                callback_data=f"pre_del_q_{q['id']}_{cat_id}_{owner_id}"
            ))
    
    # تصحيح زر الرجوع ليعود للقائمة السابقة
    kb.add(InlineKeyboardButton("🔙 رجوع", callback_data=f"manage_questions_{cat_id}_{owner_id}"))
    await c.message.edit_text("🗑️ اختر السؤال المراد حذفه:", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith('pre_del_q_'))
async def confirm_delete_question(c: types.CallbackQuery):
    data = c.data.split('_')
    # pre(0) _ del(1) _ q(2) _ qid(3) _ catid(4) _ ownerid(5)
    q_id, cat_id, owner_id = data[3], data[4], data[5]

    if c.from_user.id != int(owner_id):
        return await c.answer("⚠️ مبعسس؟ ما تقدر تحذف! 😂", show_alert=True)
    
    kb = InlineKeyboardMarkup(row_width=2).add(
        InlineKeyboardButton("✅ نعم، احذف", callback_data=f"final_del_q_{q_id}_{cat_id}_{owner_id}"),
        InlineKeyboardButton("❌ تراجع", callback_data=f"del_qs_menu_{cat_id}_{owner_id}")
    )
    await c.message.edit_text("⚠️ هل أنت متأكد من حذف هذا السؤال؟", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith('final_del_q_'))
async def execute_delete_question(c: types.CallbackQuery):
    data = c.data.split('_')
    # final(0) _ del(1) _ q(2) _ qid(3) _ catid(4) _ ownerid(5)
    q_id, cat_id, owner_id = data[3], data[4], data[5]
    
    supabase.table("questions").delete().eq("id", q_id).execute()
    await c.answer("🗑️ تم الحذف بنجاح", show_alert=True)
    
    # تحديث البيانات في الـ Callback لاستدعاء القائمة مجدداً
    c.data = f"del_qs_menu_{cat_id}_{owner_id}"
    await delete_questions_menu(c)


# --- 7. حذف القسم نهائياً (النسخة المصلحة) ---
@dp.callback_query_handler(lambda c: c.data.startswith('confirm_del_cat_'))
async def confirm_delete_cat(c: types.CallbackQuery):
    data = c.data.split('_')
    cat_id = data[3]
    owner_id = int(data[4])

    if c.from_user.id != owner_id:
        return await c.answer("⚠️ لا تملك صلاحية حذف الأقسام!", show_alert=True)

    await c.answer()
    kb = InlineKeyboardMarkup(row_width=2).add(
        InlineKeyboardButton("✅ نعم، احذف", callback_data=f"final_del_cat_{cat_id}_{owner_id}"),
        InlineKeyboardButton("❌ لا، تراجع", callback_data=f"manage_questions_{cat_id}_{owner_id}")
    )
    # تعديل نص الرسالة الحالية لطلب التأكيد
    await c.message.edit_text("⚠️ هل أنت متأكد من حذف هذا القسم نهائياً مع كل أسئلته؟", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data.startswith('final_del_cat_'))
async def execute_delete_cat(c: types.CallbackQuery):
    data = c.data.split('_')
    cat_id = data[3]
    owner_id = int(data[4])

    # 1. تنفيذ الحذف في سوبابيس
    try:
        supabase.table("categories").delete().eq("id", cat_id).execute()
        await c.answer("🗑️ تم حذف القسم بنجاح", show_alert=True)
    except Exception as e:
        return await c.answer("❌ فشل الحذف من قاعدة البيانات")

    # 2. العودة لقائمة الأقسام بتحديث نفس الرسالة
    # استخدمنا await لضمان التنفيذ وتمرير المتغيرات لعمل Edit
    await custom_add_menu(c)
    
# --- 8. نظام عرض قائمة الأقسام (تصفية وحماية) ---
@dp.callback_query_handler(lambda c: c.data.startswith('list_cats_'))
async def list_categories_for_questions(c: types.CallbackQuery):
    try:
        # استخراج الآيدي من الكولباك لضمان الحماية
        owner_id = int(c.data.split('_')[-1])
        
        if c.from_user.id != owner_id:
            return await c.answer("⚠️ لا يمكنك استعراض أقسام غيرك!", show_alert=True)

        await c.answer()
        
        # طلب الأقسام التي تخص هذا المستخدم فقط من سوبابيس
        res = supabase.table("categories").select("*").eq("created_by", str(owner_id)).execute()
        categories = res.data

        if not categories:
            # إذا لم يكن لديه أقسام، نرسل تنبيهاً ونبقى في نفس اللوحة
            return await c.answer("⚠️ ليس لديك أقسام خاصة بك حالياً، قم بإضافة قسم أولاً.", show_alert=True)

        kb = InlineKeyboardMarkup(row_width=1)
        for cat in categories:
            # تشفير أزرار الأقسام بآيدي القسم وآيدي المالك
            # manage_questions_CATID_OWNERID
            kb.add(InlineKeyboardButton(
                f"📂 {cat['name']}", 
                callback_data=f"manage_questions_{cat['id']}_{owner_id}"
            ))

        # زر الرجوع للوحة "إضافة خاصة" بآيدي المستخدم
        kb.add(InlineKeyboardButton("⬅️ الرجوع", callback_data=f"custom_add_{owner_id}"))
        
        await c.message.edit_text("📋 اختر أحد أقسامك لإدارة الأسئلة:", reply_markup=kb)

    except Exception as e:
        logging.error(f"Filter Error: {e}")
        await c.answer("⚠️ حدث خطأ في جلب الأقسام.")

# --- 1. واجهة تهيئة المسابقة (النسخة النظيفة والمحمية) ---
@dp.callback_query_handler(lambda c: c.data.startswith('setup_quiz'), state="*")
async def setup_quiz_main(c: types.CallbackQuery, state: FSMContext):
    await state.finish()
    
    # تحديد الهوية: هل هو ضغط مباشر أم قادم من زر رجوع مشفر؟
    data_parts = c.data.split('_')
    owner_id = int(data_parts[-1]) if len(data_parts) > 1 else c.from_user.id
    
    # حماية المبعسسين
    if c.from_user.id != owner_id:
        return await c.answer("⚠️ اللوحة مش حقك يا حبيبنا 😂", show_alert=True)
    
    await c.answer()
    
    # حفظ صاحب الجلسة في الـ State
    await state.update_data(owner_id=owner_id, owner_name=c.from_user.first_name)
    
    text = "🎉 **أهلاً بك!**\nقم بتهيئة المسابقة عن طريق اختيار مصدر الأسئلة:"
    
    # هنا الحذف والاستدعاء: استدعينا الدالة من قسم المساعدة
    await c.message.edit_text(
        text, 
        reply_markup=get_setup_quiz_kb(owner_id), 
        parse_mode="Markdown"
    )
# ==========================================
# 1. اختيار مصدر الأسئلة (رسمي / خاص / أعضاء) - نسخة المجلدات والأسماء
# ==========================================
# --- [ أسئلة البوت: نظام المجلدات الجديد ] --
@dp.callback_query_handler(lambda c: c.data.startswith('bot_setup_step1_'), state="*")
async def start_bot_selection(c: types.CallbackQuery, state: FSMContext):
    owner_id = int(c.data.split('_')[-1])
    if c.from_user.id != owner_id: return await c.answer("⚠️ اللوحة محمية!", show_alert=True)
    
    # جلب المجلدات بدلاً من الأقسام مباشرة
    res = supabase.table("folders").select("id, name").execute()
    if not res.data: return await c.answer("⚠️ لا توجد مجلدات رسمية!", show_alert=True)

    eligible_folders = [{"id": str(item['id']), "name": item['name']} for item in res.data]
    
    # تخزين البيانات في الحالة للبدء باختيار المجلدات
    await state.update_data(
        eligible_folders=eligible_folders, 
        selected_folders=[], 
        is_bot_quiz=True, 
        current_owner_id=owner_id
    ) 
    
    # استدعاء دالة عرض المجلدات
    await render_folders_list(c.message, eligible_folders, [], owner_id)

# --- [ أسئلة خاصة: جلب أقسام المستخدم نفسه ] ---
@dp.callback_query_handler(lambda c: c.data.startswith('my_setup_step1_'), state="*")
async def start_private_selection(c: types.CallbackQuery, state: FSMContext):
    owner_id = int(c.data.split('_')[-1])
    if c.from_user.id != owner_id: return await c.answer("⚠️ اللوحة محمية!", show_alert=True)
    
    res = supabase.table("categories").select("*").eq("created_by", str(owner_id)).execute()
    if not res.data: return await c.answer("⚠️ ليس لديك أقسام خاصة!", show_alert=True)
    
    await state.update_data(eligible_cats=res.data, selected_cats=[], is_bot_quiz=False, current_owner_id=owner_id) 
    await render_categories_list(c.message, res.data, [], owner_id)


    # --- [ أسئلة الأعضاء: إظهار الأسماء بدلاً من الأرقام ] ---
@dp.callback_query_handler(lambda c: c.data.startswith('members_setup_step1_'), state="*")
async def start_member_selection(c: types.CallbackQuery, state: FSMContext):
    owner_id = int(c.data.split('_')[-1])
    if c.from_user.id != owner_id: return await c.answer("⚠️ اللوحة محمية!", show_alert=True)
    
    # جلب المعرفات التي لها أسئلة
    res = supabase.table("questions").select("created_by").execute()
    if not res.data: return await c.answer("⚠️ لا يوجد أعضاء حالياً.", show_alert=True)
    
    from collections import Counter
    counts = Counter([q['created_by'] for q in res.data])
    eligible_ids = [m_id for m_id, count in counts.items() if count >= 15]
    
    if not eligible_ids: return await c.answer("⚠️ لا يوجد مبدعون وصلوا لـ 15 سؤال.", show_alert=True)
    
    # الإصلاح: جلب الأسماء من جدول المستخدمين (users) لربط الـ ID بالاسم
    users_res = supabase.table("users").select("user_id, name").in_("user_id", eligible_ids).execute()
    
    # تحويل البيانات لقائمة كائنات تحتوي على الاسم والمعرف
    eligible_list = [{"id": str(u['user_id']), "name": u['name'] or f"مبدع {u['user_id']}"} for u in users_res.data]
    
    await state.update_data(eligible_list=eligible_list, selected_members=[], is_bot_quiz=False, current_owner_id=owner_id)
    await render_members_list(c.message, eligible_list, [], owner_id)
# ==========================================
# 2. معالجات التبديل والاختيار (Toggle & Go) - نسخة المجلدات المحدثة
# ==========================================
@dp.callback_query_handler(lambda c: c.data.startswith('toggle_folder_'), state="*")
async def toggle_folder_selection(c: types.CallbackQuery, state: FSMContext):
    data_parts = c.data.split('_')
    f_id = data_parts[2]
    owner_id = int(data_parts[3])
    
    if c.from_user.id != owner_id: 
        return await c.answer("⚠️ مبعسس؟ المجلدات لصاحب المسابقة بس! 😂", show_alert=True)
    
    data = await state.get_data()
    selected = data.get('selected_folders', [])
    eligible = data.get('eligible_folders', [])
    
    if f_id in selected: selected.remove(f_id)
    else: selected.append(f_id)
    
    await state.update_data(selected_folders=selected)
    await c.answer()
    # استدعاء دالة رندر المجلدات لتحديث الشكل
    await render_folders_list(c.message, eligible, selected, owner_id)

 # --- [ 2. معالج الانتقال من المجلدات إلى الأقسام ] ---
@dp.callback_query_handler(lambda c: c.data.startswith('confirm_folders_'), state="*")
async def confirm_folders_to_cats(c: types.CallbackQuery, state: FSMContext):
    owner_id = int(c.data.split('_')[-1])
    if c.from_user.id != owner_id: return await c.answer("⚠️ اللوحة محمية!", show_alert=True)
    
    data = await state.get_data()
    chosen_folder_ids = data.get('selected_folders', [])
    
    if not chosen_folder_ids:
        return await c.answer("⚠️ اختر مجلد واحد على الأقل!", show_alert=True)

    # جلب الأقسام التابعة للمجلدات المختارة فقط من جدول bot_categories
    res = supabase.table("bot_categories").select("id, name").in_("folder_id", chosen_folder_ids).execute()
    
    if not res.data:
        return await c.answer("⚠️ هذه المجلدات لا تحتوي على أقسام حالياً!", show_alert=True)
    
    await state.update_data(eligible_cats=res.data, selected_cats=[])
    await c.answer("✅ تم جلب أقسام المجلدات")
    # الانتقال لعرض الأقسام
    await render_categories_list(c.message, res.data, [], owner_id)

# --- [ 3. معالج تبديل الأعضاء (Members Toggle) ] ---
@dp.callback_query_handler(lambda c: c.data.startswith('toggle_mem_'), state="*")
async def toggle_member(c: types.CallbackQuery, state: FSMContext):
    data_parts = c.data.split('_')
    m_id = data_parts[2]
    owner_id = int(data_parts[3])
    
    if c.from_user.id != owner_id: return await c.answer("⚠️ مبعسس؟ ما تقدر تختار! 😂", show_alert=True)
    
    data = await state.get_data()
    selected = data.get('selected_members', [])
    eligible = data.get('eligible_list', []) # تحتوي على الأوبجكت {id, name}
    
    if m_id in selected: selected.remove(m_id)
    else: selected.append(m_id)
    
    await state.update_data(selected_members=selected)
    await c.answer()
    await render_members_list(c.message, eligible, selected, owner_id)

# --- [ 4. معالج الانتقال من الأعضاء إلى الأقسام ] ---
@dp.callback_query_handler(lambda c: c.data.startswith('go_to_cats_step_'), state="*")
async def show_selected_members_cats(c: types.CallbackQuery, state: FSMContext):
    owner_id = int(c.data.split('_')[-1])
    if c.from_user.id != owner_id: return await c.answer("⚠️ اللوحة ليست لك!", show_alert=True)
    
    data = await state.get_data()
    chosen_ids = data.get('selected_members', [])
    
    # جلب الأقسام الخاصة بالأعضاء المختارين
    res = supabase.table("categories").select("id, name").in_("created_by", chosen_ids).execute()
    
    await state.update_data(eligible_cats=res.data, selected_cats=[])
    await render_categories_list(c.message, res.data, [], owner_id)

# --- [ 5. معالج تبديل الأقسام (Categories Toggle) ] ---
@dp.callback_query_handler(lambda c: c.data.startswith('toggle_cat_'), state="*")
async def toggle_category_selection(c: types.CallbackQuery, state: FSMContext):
    data_parts = c.data.split('_')
    cat_id = data_parts[2]
    owner_id = int(data_parts[3])
    
    if c.from_user.id != owner_id: return await c.answer("⚠️ اللوحة محمية!", show_alert=True)

    data = await state.get_data()
    selected = data.get('selected_cats', [])
    eligible = data.get('eligible_cats', [])
    
    if cat_id in selected: selected.remove(cat_id)
    else: selected.append(cat_id)
    
    await state.update_data(selected_cats=selected)
    await c.answer()
    await render_categories_list(c.message, eligible, selected, owner_id)
# --- 4. لوحة الإعدادات (استدعاء دالة المساعدة) ---
@dp.callback_query_handler(lambda c: c.data.startswith('final_quiz_settings'), state="*")
async def final_quiz_settings_panel(c: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    # جلب owner_id من البيانات المخزنة لضمان الحماية
    owner_id = data.get('current_owner_id') or c.from_user.id
    
    if c.from_user.id != owner_id:
        return await c.answer("⚠️ هذه اللوحة محمية لصاحب المسابقة!", show_alert=True)
    
    await c.answer()
    # استدعاء دالة العرض من قسم المساعدة
    await render_final_settings_panel(c.message, data, owner_id)
    
# --- [ 5 + 6 ] المحرك الموحد المطور بنظام التدوير الذكي (2026) --- #
@dp.callback_query_handler(lambda c: c.data.startswith(('tog_', 'cyc_', 'start_quiz_')), state="*")
async def quiz_settings_engines(c: types.CallbackQuery, state: FSMContext):
    data_parts = c.data.split('_')
    action = data_parts[0] 
    owner_id = int(data_parts[-1])
    
    # الدرع الأمني
    if c.from_user.id != owner_id:
        return await c.answer("⚠️ لا تتدخل في إعدادات غيرك! 😂", show_alert=True)

    data = await state.get_data()

    # 1️⃣ --- قسم محركات التدوير (التعديل اللحظي) ---
    if action in ['tog', 'cyc']:
        target = data_parts[1]
        
        # 🔄 تدوير عدد الأسئلة (10 إلى 80)
        if target == 'cnt':
            counts = [10, 15, 20, 25, 30, 35, 40, 50, 60, 70, 80]
            curr = data.get('quiz_count', 10)
            next_val = counts[(counts.index(curr) + 1) % len(counts)] if curr in counts else 10
            await state.update_data(quiz_count=next_val)
            await c.answer(f"📊 الأسئلة: {next_val}")

        # ⏳ تدوير الوقت (10 إلى 60)
        elif target == 'time':
            times = [10, 15, 20, 30, 45, 60]
            curr = data.get('quiz_time', 15)
            next_val = times[(times.index(curr) + 1) % len(times)] if curr in times else 15
            await state.update_data(quiz_time=next_val)
            await c.answer(f"⏱️ الوقت: {next_val} ثانية")

        # 🎨 تدوير نمط العرض (3 خيارات: اختيارات، مباشرة، الكل)
        elif target == 'style':
            styles = ["اختيارات 📊", "مباشرة ⚡", "الكل 📋"]
            curr = data.get('quiz_style', "اختيارات 📊")
            next_val = styles[(styles.index(curr) + 1) % len(styles)] if curr in styles else "اختيارات 📊"
            await state.update_data(quiz_style=next_val)
            await c.answer(f"🎨 العرض: {next_val}")

        # 💡 تدوير التلميح
        elif target == 'hint':
            is_on = data.get('quiz_hint_bool', False)
            await state.update_data(quiz_hint_bool=not is_on, quiz_smart_bool=not is_on)
            await c.answer("✅ تلميح مفعل" if not is_on else "❌ تلميح معطل")

        # 🔖 تدوير نظام اللعب
        elif target == 'mode':
            curr_m = data.get('quiz_mode', 'السرعة ⚡')
            next_m = 'الوقت الكامل ⏳' if curr_m == 'السرعة ⚡' else 'السرعة ⚡'
            await state.update_data(quiz_mode=next_m)
            await c.answer(f"🔖 النظام: {next_m}")

        # 🌐 تدوير النطاق
        elif target == 'broad':
            current_broad = data.get('is_broadcast', False)
            await state.update_data(is_broadcast=not current_broad)
            await c.answer("🌐 عامة" if not current_broad else "📍 داخلية")

        # تحديث اللوحة فوراً بعد أي تغيير
        new_data = await state.get_data()
        return await render_final_settings_panel(c.message, new_data, owner_id)

    # 2️⃣ --- قسم بدء الحفظ والتشغيل ---
    elif action == 'start' and data_parts[1] == 'quiz':
        is_broadcast = data.get('is_broadcast', False)
        if is_broadcast:
            res = supabase.table("groups_hub").select("group_id").eq("status", "active").execute()
            if not res.data:
                return await c.answer("❌ لا توجد قروبات مفعلة للإذاعة!", show_alert=True)
        
        await Form.waiting_for_quiz_name.set() 
        return await c.message.edit_text(
            "📝 **يا بطل، أرسل الآن اسماً لمسابقتك:**\n\n"
            "*(سيتم حفظ الوقت والنمط المختار تحت هذا الاسم)*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton("❌ إلغاء", callback_data=f"close_{owner_id}")
            )
        )
@dp.message_handler(state=Form.waiting_for_quiz_name)
async def process_quiz_name_final(message: types.Message, state: FSMContext):
    quiz_name = message.text.strip()
    data = await state.get_data()
    
    selected_cats = data.get('selected_cats', [])
    clean_list = [str(c) for c in selected_cats] 
    u_id = str(message.from_user.id)

    # تجهيز البيانات (Payload) - سحب القيم من نظام التدوير (Cycling)
    payload = {
        "created_by": u_id,
        "quiz_name": quiz_name,
        "chat_id": u_id,
        "time_limit": int(data.get('quiz_time', 15)),
        "questions_count": int(data.get('quiz_count', 10)),
        "mode": data.get('quiz_mode', 'السرعة ⚡'),
        "quiz_style": data.get('quiz_style', 'اختيارات 📊'), # حفظ النمط (اختيارات/مباشرة/الكل)
        "hint_enabled": bool(data.get('quiz_hint_bool', False)),
        "smart_hint": bool(data.get('quiz_smart_bool', False)),
        "is_bot_quiz": bool(data.get('is_bot_quiz', False)),
        "cats": json.dumps(clean_list), 
        "is_public": bool(data.get('is_broadcast', False)) 
    }

    try:
        # تنفيذ الحفظ في سوبابيس (Supabase)
        supabase.table("saved_quizzes").insert(payload).execute()
        
        # تنسيق رسالة النجاح النهائية
        is_pub = payload["is_public"]
        q_style = payload["quiz_style"]
        scope_emoji = "🌐" if is_pub else "📍"
        scope_text = "إذاعة عامة" if is_pub else "مسابقة داخلية"
        
        success_msg = (
            f"✅ **تم حفظ المسابقة بنجاح!**\n"
            f"❃┅┅┅┄┄┄┈•❃•┈┄┄┄┅┅┅❃\n"
            f"🏷 الاسم: `{quiz_name}`\n"
            f"⏱ الوقت: `{payload['time_limit']} ثانية`\n"
            f"📊 العدد: `{payload['questions_count']} سؤال`\n"
            f"🎨 العرض: **{q_style}**\n" 
            f"🔖 النظام: **{payload['mode']}**\n"
            f"{scope_emoji} النطاق: **{scope_text}**\n"
            f"❃┅┅┅┄┄┄┈•❃•┈┄┄┄┅┅┅❃\n\n"
            f"🚀 اكتب كلمة **مسابقة** ستجدها الآن في 'قائمة مسابقاتك' جاهزة للانطلاق!"
        )
        
        await message.answer(success_msg, parse_mode="Markdown")
        await state.finish() # إنهاء الحالة بنجاح

    except Exception as e:
        import logging
        logging.error(f"Error saving quiz: {e}")
        await message.answer(f"❌ خطأ في قاعدة البيانات:\n`{str(e)}`", parse_mode="Markdown")
# ==========================================
# [1] عرض قائمة المسابقات (نسخة ياسر المصفاة)
# ==========================================
@dp.message_handler(lambda message: message.text == "مسابقة")
@dp.callback_query_handler(lambda c: c.data.startswith('list_my_quizzes_'), state="*")
async def show_quizzes(obj):
    is_callback = isinstance(obj, types.CallbackQuery)
    user = obj.from_user
    u_id = str(user.id)
    
    # جلب المسابقات الخاصة بالمستخدم فقط من سوبابيس
    res = supabase.table("saved_quizzes").select("*").eq("created_by", u_id).execute()
    kb = InlineKeyboardMarkup(row_width=1)
    
    if not res.data:
        msg_empty = f"⚠️ يا {user.first_name}، لا توجد لديك مسابقات محفوظة.**"
        if is_callback: return await obj.message.edit_text(msg_empty)
        return await obj.answer(msg_empty)

    # بناء قائمة المسابقات
    for q in res.data:
        kb.add(InlineKeyboardButton(
            f"🏆 {q['quiz_name']}", 
            callback_data=f"manage_quiz_{q['id']}_{u_id}"
        ))
    
    kb.add(InlineKeyboardButton("❌ إغلاق", callback_data=f"close_{u_id}"))
    
    title = f"🎁 مسابقاتك الجاهزة يا {user.first_name}:"

    if is_callback:
        await obj.message.edit_text(title, reply_markup=kb, parse_mode="Markdown")
    else:
        await obj.reply(title, reply_markup=kb, parse_mode="Markdown")

# ==========================================
# [2] المحرك الأمني ولوحة التحكم (التشطيب النهائي المصلح)
# ==========================================
@dp.callback_query_handler(lambda c: c.data.startswith(('run_', 'close_', 'confirm_del_', 'final_del_', 'toggle_time_', 'toggle_count_', 'manage_quiz_', 'quiz_settings_', 'set_c_', 'set_t_', 'toggle_speed_', 'toggle_scope_', 'toggle_hint_', 'toggle_style_', 'save_quiz_process_')), state="*")
async def handle_secure_actions(c: types.CallbackQuery, state: FSMContext):
    try:
        data_parts = c.data.split('_')
        owner_id = data_parts[-1]
        user_id = str(c.from_user.id)
        
        # 🛡️ الدرع الأمني: التأكد أن المستخدم هو صاحب اللوحة
        if user_id != owner_id:
            return await c.answer("🚫 هذه اللوحة ليست لك.", show_alert=True)

        # 1️⃣ شاشة الإدارة الرئيسية للمسابقة
        if c.data.startswith('manage_quiz_'):
            quiz_id = data_parts[2]
            res = supabase.table("saved_quizzes").select("quiz_name").eq("id", quiz_id).single().execute()
            
            kb = InlineKeyboardMarkup(row_width=1).add(
                InlineKeyboardButton("🚀 بدء الانطلاق", callback_data=f"run_{quiz_id}_{user_id}"),
                InlineKeyboardButton("⚙️ الإعدادات", callback_data=f"quiz_settings_{quiz_id}_{user_id}"),
                InlineKeyboardButton("🔙 رجوع", callback_data=f"list_my_quizzes_{user_id}")
            )
            await c.message.edit_text(f"💎 إدارة مسابقة: {res.data['quiz_name']}", reply_markup=kb)
            return

        # 2️⃣ لوحة الإعدادات المطورة بنظام التدوير (تحديث 2026)
        elif c.data.startswith('quiz_settings_'):
            quiz_id = data_parts[2]
            res = supabase.table("saved_quizzes").select("*").eq("id", quiz_id).single().execute()
            q = res.data
            
            # تحديث بيانات الحالة (State)
            await state.update_data(editing_quiz_id=quiz_id, quiz_name=q['quiz_name'])
            
            q_time = q.get('time_limit', 15)
            q_count = q.get('questions_count', 10)
            q_mode = q.get('mode', 'السرعة ⚡')
            q_style = q.get('quiz_style', 'اختيارات 📊') # النمط الثلاثي الجديد
            is_hint = q.get('smart_hint', False)
            is_public = q.get('is_public', False)

            text = (
                f"❃┏━ إعدادات: {q['quiz_name']} ━┓❃\n"
                f"📊 عدد الاسئلة: `{q_count}`\n"
                f"⏳ المهلة: `{q_time} ثانية`\n"
                f"🎨 العرض: `{q_style}`\n"
                f"🔖 النظام: `{q_mode}`\n"
                f"📡 النطاق: `{'عام 🌐' if is_public else 'داخلي 📍'}`\n"
                f"💡 التلميح: `{'مفعل ✅' if is_hint else 'معطل ❌'}`\n"
                "❃┗━━━━━━━━━━━━━━━┛❃"
            )

            kb = InlineKeyboardMarkup(row_width=2)
            
            # --- [ أزرار التدوير الذكية ] ---
            kb.row(
                # زر تدوير عدد الأسئلة
                InlineKeyboardButton(f"📊 الأسئلة: {q_count}", callback_data=f"toggle_count_{quiz_id}_{user_id}"),
                # زر تدوير الوقت
                InlineKeyboardButton(f"⏱️ الوقت: {q_time}ث", callback_data=f"toggle_time_{quiz_id}_{user_id}")
            )
            # زر تدوير نمط العرض (اختيارات -> مباشرة -> الكل)
            kb.row(InlineKeyboardButton(f"🎨 العرض: {q_style}", callback_data=f"toggle_style_{quiz_id}_{user_id}"))

            kb.row(
                InlineKeyboardButton(f"🔖 {q_mode}", callback_data=f"toggle_speed_{quiz_id}_{user_id}"),
                InlineKeyboardButton(f"💡 التلميح: {'✅' if is_hint else '❌'}", callback_data=f"toggle_hint_{quiz_id}_{user_id}")
            )
            
            kb.row(InlineKeyboardButton(f"📡 {'نطاق: عام 🌐' if is_public else 'نطاق: داخلي 📍'}", callback_data=f"toggle_scope_{quiz_id}_{user_id}"))
            
            kb.row(InlineKeyboardButton("💾 حفظ التعديلات 🚀", callback_data=f"save_quiz_process_{quiz_id}_{user_id}"))
            
            kb.row(
                InlineKeyboardButton("🗑️ حذف المسابقة", callback_data=f"confirm_del_{quiz_id}_{user_id}"),
                InlineKeyboardButton("🔙 رجوع", callback_data=f"manage_quiz_{quiz_id}_{user_id}")
            )
            
            await c.message.edit_text(text, reply_markup=kb, parse_mode="Markdown")
            return

        # 3️⃣ محرك التبديلات المطور (نسخة الإصلاح النهائي 2026)
        elif any(c.data.startswith(x) for x in ['toggle_hint_', 'toggle_speed_', 'toggle_scope_', 'toggle_style_', 'toggle_count_', 'toggle_time_']):
            quiz_id = data_parts[2]
            
            # 📊 تدوير عدد الأسئلة (10 إلى 80)
            if 'toggle_count_' in c.data:
                counts = [10, 15, 20, 25, 30, 35, 40, 50, 60, 70, 80]
                res = supabase.table("saved_quizzes").select("questions_count").eq("id", quiz_id).single().execute()
                curr = res.data.get('questions_count', 10)
                next_val = counts[(counts.index(curr) + 1) % len(counts)] if curr in counts else 10
                supabase.table("saved_quizzes").update({"questions_count": next_val}).eq("id", quiz_id).execute()
                await c.answer(f"📊 الأسئلة: {next_val}")

            # ⏱️ تدوير الوقت (10 إلى 60)
            elif 'toggle_time_' in c.data:
                times = [10, 15, 20, 30, 45, 60]
                res = supabase.table("saved_quizzes").select("time_limit").eq("id", quiz_id).single().execute()
                curr = res.data.get('time_limit', 15)
                next_val = times[(times.index(curr) + 1) % len(times)] if curr in times else 15
                supabase.table("saved_quizzes").update({"time_limit": next_val}).eq("id", quiz_id).execute()
                await c.answer(f"⏱️ الوقت: {next_val}ث")

            # 🎨 تدوير نمط العرض (اختيارات -> مباشرة -> الكل)
            elif 'toggle_style_' in c.data:
                styles = ["اختيارات 📊", "مباشرة ⚡", "الكل 📋"]
                res = supabase.table("saved_quizzes").select("quiz_style").eq("id", quiz_id).single().execute()
                curr = res.data.get('quiz_style', "اختيارات 📊")
                next_val = styles[(styles.index(curr) + 1) % len(styles)] if curr in styles else "اختيارات 📊"
                supabase.table("saved_quizzes").update({"quiz_style": next_val}).eq("id", quiz_id).execute()
                await c.answer(f"🎨 العرض: {next_val}")
                                                    
            # 🎨 تدوير نمط العرض (اختيارات -> مباشرة -> الكل)
            elif 'toggle_style_' in c.data:
                styles = ["اختيارات 📊", "مباشرة ⚡", "الكل 📋"]
                res = supabase.table("saved_quizzes").select("quiz_style").eq("id", quiz_id).single().execute()
                curr = res.data.get('quiz_style', "اختيارات 📊")
                next_val = styles[(styles.index(curr) + 1) % len(styles)] if curr in styles else "اختيارات 📊"
                supabase.table("saved_quizzes").update({"quiz_style": next_val}).eq("id", quiz_id).execute()
                await c.answer(f"🎨 العرض: {next_val}")

            # 📡 محرك النطاق (عام / داخلي)
            elif 'toggle_scope_' in c.data:
                res = supabase.table("saved_quizzes").select("is_public").eq("id", quiz_id).single().execute()
                new_val = not res.data.get('is_public', False)
                supabase.table("saved_quizzes").update({"is_public": new_val}).eq("id", quiz_id).execute()
                await c.answer("🌐 أصبح عاماً" if new_val else "📍 أصبح داخلياً")

            # 💡 محرك التلميح
            elif 'toggle_hint_' in c.data:
                res = supabase.table("saved_quizzes").select("smart_hint").eq("id", quiz_id).single().execute()
                new_val = not res.data.get('smart_hint', False)
                supabase.table("saved_quizzes").update({"smart_hint": new_val}).eq("id", quiz_id).execute()
                await c.answer("💡 تم تحديث التلميح")

            # 🔖 محرك النظام
            elif 'toggle_speed_' in c.data:
                res = supabase.table("saved_quizzes").select("mode").eq("id", quiz_id).single().execute()
                new_val = "الوقت الكامل ⏳" if res.data.get('mode') == "السرعة ⚡" else "السرعة ⚡"
                supabase.table("saved_quizzes").update({"mode": new_val}).eq("id", quiz_id).execute()
                await c.answer(f"🔖 النظام: {new_val}")

            # 🔄 تحديث الواجهة فوراً (ليظهر الرقم الجديد في الزر)
            c.data = f"quiz_settings_{quiz_id}_{user_id}"
            return await handle_secure_actions(c, state)
            
        # 4️⃣ الحفظ والعمليات النهائية (تبدأ من السطر 8 كما طلبت)
        elif c.data.startswith('save_quiz_process_'):
            quiz_id = data_parts[3] 
            await c.answer("✅ تم حفظ التعديلات بنجاح!", show_alert=True)
            c.data = f"manage_quiz_{quiz_id}_{user_id}"
            return await handle_secure_actions(c, state)

        elif c.data.startswith('close_'):
            try: return await c.message.delete()
            except: pass

        elif c.data.startswith('confirm_del_'):
            quiz_id = data_parts[2]
            kb = InlineKeyboardMarkup().add(
                InlineKeyboardButton("✅ نعم، احذف", callback_data=f"final_del_{quiz_id}_{user_id}"),
                InlineKeyboardButton("🚫 تراجع", callback_data=f"quiz_settings_{quiz_id}_{user_id}")
            )
            return await c.message.edit_text("⚠️ **هل أنت متأكد من حذف هذه المسابقة؟**", reply_markup=kb)

        elif c.data.startswith('final_del_'):
            quiz_id = data_parts[2]
            supabase.table("saved_quizzes").delete().eq("id", quiz_id).execute()
            await c.answer("🗑️ تم الحذف بنجاح", show_alert=True)
            c.data = f"list_my_quizzes_{user_id}"
            return await show_my_quizzes(c)
        
            # 2. العودة للقائمة: تغيير الداتا واستدعاء دالة العرض مباشرة
            c.data = f"list_my_quizzes_{user_id}"
            return await show_my_quizzes(c) 

        # --- [ نظام تشغيل المسابقات: عامة أو خاصة ] ---
        elif c.data.startswith('run_'):
            quiz_id = data_parts[1]
            user_id = data_parts[2]
            
            res = supabase.table("saved_quizzes").select("*").eq("id", quiz_id).single().execute()
            q_data = res.data
            
            if not q_data: 
                return await c.answer("❌ المسابقة غير موجودة!")

            # 🔥 حل مشكلة عدم حذف اللوحة: نحذفها هنا قبل تشغيل أي محرك
            try:
                await c.message.delete()
            except:
                pass

            if q_data.get('is_public'):
                # 🛡️ استدعاء نقطة التفتيش الأمنية
                gate_passed = await security_checkpoint(c)
                
                if gate_passed:
                    # 🌐 مسار الإذاعة العامة (فقط في حال نجاح التفتيش)
                    await c.answer("🌐 جاري إطلاق الإذاعة العامة للمجموعات...")
                    await start_broadcast_process(c, quiz_id, user_id)
                else:
                    # تم التعامل مع الرد داخل دالة الـ checkpoint
                    return 
                
            else:
                # 📍 مسار التشغيل الخاص
                if q_data.get('is_bot_quiz'):
                    asyncio.create_task(engine_bot_questions(c.message.chat.id, q_data, c.from_user.first_name))
                else:
                    asyncio.create_task(engine_user_questions(c.message.chat.id, q_data, c.from_user.first_name))
            
            return # إنهاء المعالج بنجاح

    except Exception as e:
        logging.error(f"Handle Secure Actions Error: {e}")
        try: 
            await c.answer("🚨 خطأ في اللوحة أو البيانات", show_alert=True)
        except: 
            pass
        
# ==========================================
# 3. نظام المحركات المنفصلة (ياسر المطور - نسخة عشوائية)
# ==========================================

# --- [1. محرك أسئلة البوت] ---
async def engine_bot_questions(chat_id, quiz_data, owner_name):
    try:
        raw_cats = quiz_data.get('cats', [])
        if isinstance(raw_cats, str):
            try:
                cat_ids_list = json.loads(raw_cats)
            except:
                cat_ids_list = raw_cats.replace('[','').replace(']','').replace('"','').split(',')
        else:
            cat_ids_list = raw_cats

        cat_ids = [int(c) for c in cat_ids_list if str(c).strip().isdigit()]
        if not cat_ids:
            return await bot.send_message(chat_id, "⚠️ خطأ: لم يتم العثور على أقسام صالحة.")

        # جلب الأسئلة وخلطها عشوائياً
        res = supabase.table("bot_questions").select("*").in_("bot_category_id", cat_ids).execute()
        if not res.data:
            return await bot.send_message(chat_id, "⚠️ لم أجد أسئلة في جدول البوت.")

        questions_pool = res.data
        random.shuffle(questions_pool)
        count = int(quiz_data.get('questions_count', 10))
        selected_questions = questions_pool[:count]

        await run_universal_logic(chat_id, selected_questions, quiz_data, owner_name, "bot")
    except Exception as e:
        logging.error(f"Bot Engine Error: {e}")

# --- [2. محرك أسئلة الأعضاء] ---
async def engine_user_questions(chat_id, quiz_data, owner_name):
    try:
        raw_cats = quiz_data.get('cats', [])
        if isinstance(raw_cats, str):
            try:
                cat_ids_list = json.loads(raw_cats)
            except:
                cat_ids_list = raw_cats.replace('[','').replace(']','').replace('"','').split(',')
        else:
            cat_ids_list = raw_cats

        cat_ids = [int(c) for c in cat_ids_list if str(c).strip().isdigit()]
        if not cat_ids:
            return await bot.send_message(chat_id, "⚠️ خطأ في أقسام الأعضاء.")

        # جلب الأسئلة وخلطها عشوائياً
        res = supabase.table("questions").select("*, categories(name)").in_("category_id", cat_ids).execute()
        if not res.data:
            return await bot.send_message(chat_id, "⚠️ لم أجد أسئلة في أقسام الأعضاء.")

        questions_pool = res.data
        random.shuffle(questions_pool)
        count = int(quiz_data.get('questions_count', 10))
        selected_questions = questions_pool[:count]

        await run_universal_logic(chat_id, selected_questions, quiz_data, owner_name, "user")
    except Exception as e:
        logging.error(f"User Engine Error: {e}")


# --- [ محرك التلميحات الملكي المطور: 3 قلوب + ذاكرة سحابية ✨ ] ---

current_key_index = 0 # متغير تدوير المفاتيح
# ============================================================
# 🔄 محرك التلميحات الذكي بنظام التدوير الآلي (النسخة المطورة)
# ============================================================
async def generate_smart_hint(answer_text, question_text="سؤال غير محدد", force_refresh=False):
    answer_text = str(answer_text).strip()
    
    # 1. فحص الذاكرة السحابية (Skip if force_refresh is True)
    if not force_refresh:
        try:
            cached_res = supabase.table("hints").select("hint").eq("word", answer_text).execute()
            if cached_res.data:
                return cached_res.data[0]['hint']
        except Exception as e:
            logging.error(f"Database Cache Error: {e}")

    # 2. جلب قائمة المفاتيح المتاحة للتدوير
    available_keys = ["G_KEY_1", "G_KEY_2", "G_KEY_3"]
    
    try:
        active_res = supabase.table("system_settings").select("key_value").eq("key_name", "ACTIVE_GROQ_KEY").execute()
        if active_res.data:
            start_key = active_res.data[0]['key_value']
            if start_key in available_keys:
                available_keys.remove(start_key)
                available_keys.insert(0, start_key)
    except: 
        pass

    # 3. محرك التدوير (Rotation Loop)
    url = "https://api.groq.com/openai/v1/chat/completions"
    
    for key_alias in available_keys:
        try:
            # جلب التوكن الفعلي للمفتاح الحالي
            token_res = supabase.table("system_settings").select("key_value").eq("key_name", key_alias).execute()
            active_token = token_res.data[0]['key_value'] if token_res.data else None
            
            if not active_token:
                continue

            headers = {"Authorization": f"Bearer {active_token}", "Content-Type": "application/json"}
            
            # بناء الطلب بذكاء لمنع التكرار
            payload = {
                "model": "llama-3.3-70b-versatile",
                "messages": [
                    {
                        "role": "system", 
                        "content": "أنت خبير ألغاز محترف. مهمتك إعطاء تلميح ذكي يصف الكلمة من زاوية بعيدة تماماً عن نص السؤال الأصلي. ممنوع ذكر الإجابة."
                    },
                    {
                        "role": "user", 
                        "content": (
                            f"السؤال الأصلي: ({question_text})\n"
                            f"الكلمة المستهدفة: ({answer_text})\n\n"
                            "أعطني وصفاً قصيراً ومسلياً لا يتجاوز 10 كلمات، يلمح للكلمة بذكاء دون تكرار كلمات السؤال."
                        )
                    }
                ],
                "temperature": 0.8
            }

            async with httpx.AsyncClient() as client:
                response = await client.post(url, headers=headers, json=payload, timeout=10.0)
                
                if response.status_code == 200:
                    ai_hint = response.json()['choices'][0]['message']['content'].strip()
                    # تنظيف النص
                    ai_hint = ai_hint.replace('"', '').replace('«', '').replace('»', '')
                    
                    final_hint = (
                        f" <b>〔 تـلـمـيـح ذكـي 〕</b> \n"
                        f"   <b>📜 الوصف:</b>\n"
                        f"   <i>« {ai_hint} »</i>\n\n"
                        f"❃"
                    )
                    
                    # تحديث المفتاح الناجح وحفظ التلميح
                    try:
                        supabase.table("system_settings").update({"key_value": key_alias}).eq("key_name", "ACTIVE_GROQ_KEY").execute()
                        supabase.table("hints").upsert({"word": answer_text, "hint": final_hint}).execute()
                    except:
                        pass
                        
                    return final_hint
                
                else:
                    error_status = response.status_code
                    # إرسال تنبيه للمطور
                    try:
                        alert_text = (
                            f"⚠️ <b>تنبيه تعطل مفتاح!</b>\n"
                            f"📌 المفتاح: <code>{key_alias}</code>\n"
                            f"🚫 الخطأ: <code>{error_status}</code>"
                        )
                        await bot.send_message(ADMIN_ID, alert_text, parse_mode="HTML")
                    except:
                        pass
                    continue 

        except Exception as e:
            logging.error(f"Error rotating key {key_alias}: {e}")
            continue

    # 4. تلميح الطوارئ (إذا فشلت جميع المحاولات)
    return (
        f"💡 <b>〔 تلميح بسيط 〕</b>\n"
        f"<b>• الحرف الأول:</b> ( {answer_text[0]} )\n"
        f"<b>• طول الكلمة:</b> {len(answer_text)} حروف"
    )
async def delete_after(message, delay):
    await asyncio.sleep(delay)
    try: 
        await message.delete()
    except Exception: 
        pass
# ==========================================
# [2] المحرك الموحد (نسخة التشطيب الرسمي @QuizBot + العداد المطور)
# ==========================================
# ==========================================
async def run_universal_logic(chat_id, questions, quiz_data, owner_name, engine_type):
    random.shuffle(questions)
    overall_scores = {}
    
    # 1️⃣ [ التسجيل الرسمي ] - إنشاء سجل المسابقة الموحد في سوبابيس
    current_quiz_id = None
    try:
        sample_q = questions[0]
        if engine_type == "bot":
            main_cat = sample_q.get('category') or "عام"
        elif engine_type == "user":
            main_cat = sample_q['categories']['name'] if (sample_q.get('categories') and isinstance(sample_q['categories'], dict)) else "أقسام الأعضاء"
        else:
            main_cat = "قسم خاص"

        # إدخال البيانات الأولية مع الحقول الجديدة (السيادية)
        quiz_reg = supabase.table("active_quizzes").insert({
            "chat_id": chat_id,
            "quiz_name": f"مسابقة {owner_name}",
            "created_by": quiz_data.get('owner_id', 0),
            "is_global": (engine_type == "bot"),
            "is_active": True,
            "is_paused": False, # الرادار يبدأ أخضر 🟢
            "category_name": main_cat,
            "quiz_type": "private",
            "quiz_owner_id": quiz_data.get('owner_id'),
            "quiz_owner_name": owner_name
        }).execute()
        
        if quiz_reg.data:
            current_quiz_id = quiz_reg.data[0]['id']
            logging.info(f"✅ تم حجز ID بنجاح: {current_quiz_id}")
    except Exception as e:
        logging.error(f"❌ فشل تسجيل المسابقة: {e}")

    questions_to_delete = []
    results_to_delete = []

    # 🟢 الدخول في حلقة الأسئلة
    for i, q in enumerate(questions):
        # [أ] استخراج الإجابة والقسم بناءً على نوع المحرك
        if engine_type == "bot":
            ans = str(q.get('correct_answer') or "").strip()
            cat_name = q.get('category') or "بوت"
        elif engine_type == "user":
            ans = str(q.get('answer_text') or q.get('correct_answer') or "").strip()
            cat_name = q['categories']['name'] if (q.get('categories') and isinstance(q['categories'], dict)) else "قسم خاص"
        else:
            ans = str(q.get('correct_answer') or q.get('ans') or "").strip()
            cat_name = "مخصص 🔒"

        # 💡 [ القاموس الموحد: sync_data ] 💡
        # هذا هو "الرادار المركزي" المطابق لجدول سوبابيس 100%
        sync_data = {
            "is_active": True,
            "is_paused": active_quizzes.get(chat_id, {}).get('is_paused', False),
            "current_answer": ans,
            "current_index": i + 1,
            "total_questions": len(questions),
            "question_category_name": cat_name,
            "quiz_style": quiz_data.get('quiz_style', 'اختيارات 📊'),
            "question_finished": False, # فتح قفل السؤال الجديد
            "hint_sent": False,
            "votes_results": {"0": 0, "1": 0, "2": 0, "3": 0},
            "voter_list": {},
            "user_choices": {}
        }

        # 2️⃣ تحديث الذاكرة النشطة (الرام المحلي للمجموعة)
        # ننسخ البيانات الموحدة ونضيف عليها مكملات البوت المحلية
        active_quizzes[chat_id] = sync_data.copy()
        active_quizzes[chat_id].update({
            "quiz_id": current_quiz_id,
            "winners": [], 
            "voted_users": [], 
            "mode": quiz_data.get('mode', 'السرعة ⚡'), 
            "raw_q_data": q,
            "ans": ans, # الحفاظ على المسمى القديم للتوافق
            "active": True # للتوافق مع شروط 'if active' القديمة
        })

        # 3️⃣ [ التحديث اللحظي لقاعدة البيانات ] 🚀
        # مزامنة القاموس الموحد مع الجدول المركزي
        if current_quiz_id:
            try:
                supabase.table("active_quizzes").update(sync_data).eq("id", current_quiz_id).execute()
                logging.info(f"🔄 تم توحيد الذاكرة للسؤال {i+1} في سوبابيس")
            except Exception as e:
                logging.error(f"❌ فشل مزامنة سوبابيس: {e}")
        
        # --- [ نظام التلميح العادي البسيط ] ---
        normal_hint_str = ""
        if quiz_data.get('smart_hint'):
            ans_str = str(ans).strip()
            # تلميح أساسي جداً (بدون إضافات) التزاماً بالمرحلة الحالية
            normal_hint_str = f"مكونة من ({len(ans_str.split())}) كلمات، تبدأ بـ ( {ans_str[0]} )"

        # 3️⃣ [ استدعاء المايسترو ]
        q_msg = await send_quiz_master(
            chat_id, 
            q, 
            i+1, 
            len(questions), 
            {
                'owner_name': owner_name, 
                'quiz_db_id': current_quiz_id,
                'mode': quiz_data['mode'], 
                'time_limit': quiz_data['time_limit'], 
                'cat_name': cat_name,
                'quiz_style': quiz_data.get('quiz_style', 'اختيارات 📊'),
                'smart_hint': quiz_data.get('smart_hint'),
                'normal_hint': normal_hint_str 
            }, 
            questions 
        )
        
        if isinstance(q_msg, types.Message):
            questions_to_delete.append(q_msg.message_id)
            active_quizzes[chat_id]['last_poll_id'] = q_msg.message_id

        # 4️⃣ [ مكبح مراقبة الوقت والانسحاب الفوري      
        # 4️⃣ [ مكبح مراقبة الوقت والانسحاب الفوري ]        
        start_time = time.time()
        t_limit = int(quiz_data.get('time_limit', 15))

        while time.time() - start_time < t_limit:
            # 🕵️ [ مكبح السيادة: فحص الانسحاب الكلي ]
            if chat_id not in active_quizzes or not active_quizzes[chat_id].get('active'):
                poll_id_to_kill = active_quizzes.get(chat_id, {}).get('last_poll_id')
                if poll_id_to_kill:
                    try: await bot.stop_poll(chat_id=chat_id, message_id=poll_id_to_kill)
                    except: pass
                logging.info(f"🛑 إيقاف نهائي للمحرك في {chat_id}")
                return # 👈 خروج نهائي من الدالة (Kill Task)

            # ⚡ [ مكبح السرعة: التحسس من سطر 'question_finished' ]
            if active_quizzes[chat_id].get('question_finished'):
                logging.info(f"⚡ تم رصد إجابة سريعة! كسر الانتظار في {chat_id}")
                break # 👈 يكسر الـ while فقط ويكمل لعرض النتائج

            await asyncio.sleep(0.1)

        # 🛑 [ إغلاق الاستطلاع وتحديث القاعدة ]
        if chat_id in active_quizzes:
            poll_id_to_stop = active_quizzes[chat_id].get('last_poll_id')
            if poll_id_to_stop:
                try:
                    await bot.stop_poll(chat_id=chat_id, message_id=poll_id_to_stop)
                    # تحديث سوبابيس لإنهاء حالة السؤال
                    if current_quiz_id:
                        supabase.table("active_quizzes").update({
                            "question_finished": True 
                        }).eq("id", current_quiz_id).execute()
                except Exception as e:
                    logging.warning(f"⚠️ الاستطلاع مغلق مسبقاً: {e}")
                    
        # 5️⃣ إنهاء السؤال وعرض النتائج
        # 5️⃣ إنهاء السؤال وحساب النقاط (النسخة النظيفة 🔥)
        if chat_id in active_quizzes:
            # 🕵️ مكبح الأمان: فحص الانسحاب
            if not active_quizzes[chat_id].get('active'):
                logging.info(f"🛑 تم إيقاف عرض النتائج في {chat_id} - انسحاب")
                return 

            # ✅ إغلاق السؤال الحالي للبدء في اللي بعده
            active_quizzes[chat_id]['question_finished'] = True
            
            # جلب الفائزين وحساب النقاط
            current_winners = active_quizzes[chat_id].get('winners', [])
            for w in current_winners:
                uid = w['id']
                if uid not in overall_scores:
                    overall_scores[uid] = {"name": w['name'], "points": 0}
                overall_scores[uid]['points'] += 1

            # عرض القالب (لنظام الكتابة والمباشر)
            current_style = active_quizzes[chat_id].get('quiz_style', '')
            if current_style != 'اختيارات 📊':
                res_msg = await send_creative_results2(chat_id, ans, current_winners, overall_scores)
                if isinstance(res_msg, types.Message):
                    results_to_delete.append(res_msg.message_id)
            else:
                logging.info(f"✨ نظام اختيارات: تسجيل الفوز صامتاً في {chat_id}")
            
            # ⏳ الفراغ الزمني (راحة 8 ثوانٍ) قبل السؤال القادم
            await asyncio.sleep(1)

        # 6️⃣ [ ⏱️ محرك العداد التنازلي المطور مع مكابح داخلية ]
        if i < len(questions) - 1:
            # 🕵️ [ مكبح المحطة الرابعة: فحص قبل بدء العداد التنازلي ]
            # هنا نتأكد أن المطور لم يوقف المسابقة أثناء عرض النتائج
            if chat_id not in active_quizzes or not active_quizzes[chat_id].get('active'):
                return

            icons = ["⚪", "🟢", "🔴"]
            try:
                countdown_msg = await bot.send_message(chat_id, f"⌛ استعدوا.. السؤال التالي يبدأ بعد قليل...")
                
                # عداد ذكي (5 ثوانٍ) يفحص "البريك" في كل ثانية
                for count in range(3, 0, -1):
                    # 🔍 التفتيش اللحظي داخل العداد
                    if chat_id not in active_quizzes or not active_quizzes[chat_id].get('active'):
                        await countdown_msg.delete()
                        logging.info(f"⚡ تم قتل العداد فوراً في {chat_id}")
                        return 
                    
                    # تحديث شكل العداد (تحديث كل ثانية ليكون أكثر فخامة)
                    icon = icons[count] if count < len(icons) else "⚪"
                    try:
                        await countdown_msg.edit_text(
                            f"{icon} استعدوا.. السؤال التالي يبدأ بعد <b>{count}</b> ثواني...",
                            parse_mode="HTML"
                        )
                    except Exception as e:
                        logging.warning(f"Flood avoidance: {e}")
                        # إذا حدث Flood، نكتفي بالانتظار صمتاً دون تحديث النص
                    
                    await asyncio.sleep(1) # الانتظار ثانية واحدة للفحص التالي
                
                await countdown_msg.delete()
            except Exception as e:
                logging.error(f"Countdown Error: {e}")
        else:
            # نهاية الأسئلة - استراحة قصيرة قبل النتائج النهائية
            await asyncio.sleep(0.3)
             
    # 7️⃣ [ إعلان لوحة الشرف النهائية 📊 ]
    target_quiz_id = active_quizzes.get(chat_id, {}).get('quiz_id')
    current_cat = active_quizzes.get(chat_id, {}).get('category', "عام")
    final_scores_from_db = {}

    if target_quiz_id:
        try:
            loop = asyncio.get_event_loop()
            # جلب البيانات من سوبابيس (نقاط + عدد الإجابات الصحيحة)
            response = await loop.run_in_executor(None, lambda: (
                supabase.table("answers_log")
                .select("user_id, user_name, points_earned, is_correct")
                .eq("quiz_id", target_quiz_id)
                .eq("is_correct", True)
                .execute()
            ))

            if response and response.data:
                for row in response.data:
                    uid = row['user_id']
                    if uid not in final_scores_from_db:
                        final_scores_from_db[uid] = {"name": row['user_name'], "points": 0, "correct_count": 0}
                    final_scores_from_db[uid]['points'] += row['points_earned']
                    final_scores_from_db[uid]['correct_count'] += 1
            else:
                # خطة الطوارئ: استخدام الذاكرة المحلية إذا تعذر الاتصال بسوبابيس
                final_scores_from_db = overall_scores.get(chat_id, {}).copy()
        except Exception as e:
            logging.error(f"❌ خطأ جلب النتائج النهائية: {e}")
            final_scores_from_db = overall_scores.get(chat_id, {}).copy()

    # 📤 إرسال لوحة الشرف النهائية (الواجهة البصرية للاعبين)
    await send_final_results2(chat_id, final_scores_from_db, len(questions))

    # 🔥 [ الجراحة السيادية: التصفير اللحظي والشامل للرام ]
    
    # 1. تصفير "ذاكرة الرصد السريع" (active_polls) لمنع تداخل الاختيارات
    active_polls_keys = [k for k, v in active_polls.items() if v.get('chat_id') == chat_id]
    for k in active_polls_keys:
        if k in active_polls:
            del active_polls[k]
    logging.info(f"🧹 تم تصفير {len(active_polls_keys)} سجل من ذاكرة الاختيارات (active_polls)")

    # 2. تصفير "الرادار المحلي" (active_quizzes)
    # 5️⃣ تنظيف بيانات السؤال (وليس المسابقة!) 🧹
    if chat_id in active_quizzes:
            # ❌ حذفنا سطر active = False وحذفنا del
            
            # ✅ نصفر فقط ما يخص السؤال الحالي للاستعداد للقادم
        active_quizzes[chat_id].update({
            "question_finished": True, # السؤال انتهى
            "winners": [],              # تصفير الفائزين للسؤال الجديد
            "voted_users": [],          # تصفير المصوتين للسؤال الجديد
            "ans": None                 # مسح الإجابة القديمة مؤقتاً
        })
        logging.info(f"🧹 تم تجهيز الرادار للسؤال القادم في {chat_id}")
            

    # 3. تصفير مصفوفة النقاط المؤقتة
    if chat_id in overall_scores:
        del overall_scores[chat_id]

    # 🚀 [ المهمة الثقيلة: التنظيف العميق والترحيل للخلفية ]
    async def final_cleanup_process(tid, cid, scores, cat, m_ids):
        try:
            # 1️⃣ المزامنة مع قاعدة البيانات العالمية (حفظ عرق اللاعبين)
            if scores:
                await sync_points_to_global_db(
                    group_scores={"special_event": scores}, 
                    winners_list=["special_event"], 
                    cat_name=cat, 
                    is_special=True
                )
            
            # 2️⃣ تدمير سجل المسابقة في سوبابيس (لإتاحة البدء من جديد)
            if tid:
                supabase.table("active_quizzes").delete().eq("id", tid).execute()
            
            # 3️⃣ مسح مخلفات الرسائل (تنظيف الدردشة)
            for m_id in m_ids:
                try: await bot.delete_message(cid, m_id)
                except: pass
                
            logging.info(f"✨ اكتمل التطهير والترحيل بنجاح للمجموعة {cid}")
        except Exception as e:
            logging.error(f"⚠️ فشل التنظيف النهائي: {e}")

    # إطلاق مهمة الخلفية مع تمرير كافة الوسائط المطلوبة
    # ✅ استدعاء واحد فقط وصحيح (يحتوي على m_ids في النهاية)
    asyncio.create_task(final_cleanup_process(
        target_quiz_id, 
        chat_id, 
        final_scores_from_db, 
        current_cat, 
        (questions_to_delete + results_to_delete) 
    ))

    # تحرير القاعة (القفل المنطقي)
    active_broadcasts.discard(chat_id)
    
    logging.info(f"✅ تم تحرير القاعة {chat_id} وجاري التنظيف في الخلفية...")

# ==========================================
# 1️⃣ صمام الأمان العالمي (خارج الدالة لمنع الطلقة المزدوجة)
active_broadcasts = set()

# 2️⃣ دالة العداد التنازلي المصححة لتجنب أي NameError
async def run_countdown(chat_id):
    try:
        msg = await bot.send_message(chat_id, "⏳ استعدوا.. السؤال القادم بعد: 3")
        for i in range(2, 0, -1):
            await asyncio.sleep(1.5)
            try: await bot.edit_message_text(f"⏳ استعدوا.. السؤال القادم بعد: {i}", chat_id, msg.message_id)
            except: pass
        await asyncio.sleep(1.5)
        try: await bot.delete_message(chat_id, msg.message_id)
        except: pass
    except: pass


# 3️⃣ المحرك الرئيسي الموحد (نسخة ياسر المطورة 2026)
async def engine_global_broadcast(chat_ids, quiz_data, owner_name, current_quiz_db_id=None):
    # 1. [ المندوب سلم القائمة ]
    input_ids = chat_ids if isinstance(chat_ids, list) else [chat_ids]
    all_chats = list(set(input_ids))
    current_quiz_db_id = None
    cat_name = "عام" 
    owner_name_str = quiz_data.get('owner_name') or "مجهول"

    # --- [ أ ] تعريف الخرائط الناقصة لضمان عدم حدوث Error ---
    # 🔥 [ إضافة قاموس الأسماء هنا ] 🔥
    group_names_map = {}
    try:
        # جلب بيانات المجموعات المشاركة دفعة واحدة لسرعة الأداء
        res = supabase.table("groups_hub").select("group_id, group_name").in_("group_id", all_chats).execute()
        # تحويل النتيجة إلى قاموس يسهل الوصول إليه: {ID: Name}
        group_names_map = {str(item['group_id']): item['group_name'] for item in res.data}
    except Exception as e:
        logging.error(f"⚠️ Error fetching group names: {e}")
    
    # تأمين وجود اسم لكل آيدي حتى لو فشل الجلب
    for cid in all_chats:
        if str(cid) not in group_names_map:
            group_names_map[str(cid)] = f"جروب {cid}"
  
    messages_to_delete = []

    # جلب أسماء المجموعات لكي نستخدمها في "الكشوفات" (answers_log)
    for cid in all_chats:
        try:
            chat_info = await bot.get_chat(cid)
            group_names_map[cid] = chat_info.title or "مجموعة مجهولة"
        except:
            group_names_map[cid] = "مجموعة خاصة"

    if not all_chats:
        logging.error("⚠️ لا توجد مجموعات صالحة للبث.")
        return

    # --- [ ب ] نظام الفلترة الذكي لضمان عدم تداخل المسابقات ---
    chats_to_broadcast = []
    for cid in all_chats:
        is_busy = (cid in active_broadcasts) or (cid in active_quizzes and active_quizzes[cid].get('active'))
        if is_busy:
            logging.warning(f"⚠️ تخطي المجموعة {cid}: لديها مسابقة نشطة بالفعل.")
            continue 
            
        chats_to_broadcast.append(cid)
        active_broadcasts.add(cid) # حجز المجموعة في الذاكرة لمنع أي تداخل

    if not chats_to_broadcast:
        logging.error("🚫 جميع المجموعات المستهدفة مشغولة حالياً.")
        return

    logging.info(f"📡 تم حجز {len(chats_to_broadcast)} مجموعة لبدء الإذاعة العالمية.")

    try:
        # --- [ ج ] جلب وتجهيز الأسئلة من المخزن ---
        raw_cats = quiz_data.get('cats', [])
        if isinstance(raw_cats, str):
            try: cat_ids_list = json.loads(raw_cats)
            except: cat_ids_list = raw_cats.replace('[','').replace(']','').replace('"','').split(',')
        else: 
            cat_ids_list = raw_cats
            
        cat_ids = [int(c) for c in cat_ids_list if str(c).strip().isdigit()]

        is_bot = quiz_data.get("is_bot_quiz", False)
        table = "bot_questions" if is_bot else "questions"
        cat_col = "bot_category_id" if is_bot else "category_id"
        current_style = quiz_data.get('quiz_style', 'السرعة ⚡') 
        
        # 1️⃣ جلب الأسئلة مع التأكد من الربط الصحيح
        res_q = supabase.table(table).select("*, categories(name)" if not is_bot else "*").in_(cat_col, cat_ids).execute()
        
        if not res_q.data:
            logging.error(f"⚠️ لم يتم العثور على أسئلة للقسم المحدد: {cat_ids}")
            return

        pool = res_q.data
        random.shuffle(pool)
        count = int(quiz_data.get('questions_count', 10))
        selected_questions = pool[:count] 
        total_q = len(selected_questions)

        # 2️⃣ استخراج اسم القسم الرئيسي (للسجل المركزي)
        sample_q = selected_questions[0]
        if is_bot:
            main_cat_name = sample_q.get('category') or "بوت"
        else:
            main_cat_name = sample_q['categories']['name'] if (sample_q.get('categories') and isinstance(sample_q['categories'], dict)) else "عام"

        group_scores = {cid: {} for cid in all_chats}
        messages_to_delete = {cid: [] for cid in all_chats}
        results_to_delete = {cid: [] for cid in all_chats}

        # 🟢 [ الخطوة 1: المشرف ] إنشاء سجل المسابقة المركزي الموحد
        current_quiz_db_id = None
        try:
            creator_id = quiz_data.get('owner_id') or quiz_data.get('created_by') or 0
            quiz_entry = supabase.table("active_quizzes").insert({
                "quiz_name": f"إذاعة {owner_name}",
                "created_by": creator_id,
                "is_global": True,
                "is_active": True,
                "is_paused": False, # الرادار يبدأ أخضر 🟢
                "participants_ids": chats_to_broadcast, # نحفظ الآيديات الصافية للمزامنة
                "total_questions": total_q,
                "quiz_type": "public",
                "category_name": main_cat_name,
                "quiz_style": current_style,
                "quiz_owner_id": creator_id,
                "quiz_owner_name": owner_name
            }).execute()

            if quiz_entry.data:
                current_quiz_db_id = quiz_entry.data[0]['id']
                logging.info(f"✅ سجل active_quizzes جاهز ID: {current_quiz_db_id}")

                # تسجيل المشاركين في الجدول الفرعي (للأرشفة)
                participants_records = [{"quiz_id": current_quiz_db_id, "chat_id": cid} for cid in chats_to_broadcast]
                supabase.table("quiz_participants").insert(participants_records).execute()

        except Exception as e:
            logging.error(f"❌ خطأ سوبابيس في مرحلة التسجيل: {e}")
            return
        # --- [ داخل دورة البث الموحدة ] ---
        # --- [ داخل دورة البث الموحدة ] ---
        for i, q in enumerate(selected_questions):
            
            # 🔥 [ 1. رادار سوبابيس اللحظي - المرجع الوحيد للحقيقة ] 🔥
            # سحب القائمة "الآن" من جدول المشاركين لضمان توقف المنسحبين يدوياً
            try:
                p_res = supabase.table("quiz_participants").select("chat_id").eq("quiz_id", current_quiz_db_id).execute()
                # قائمة المجموعات اللي لسه "مخزنة" في الجدول حالياً
                db_active_ids = [p['chat_id'] for p in p_res.data]
                
                # تحديث القائمة المحلية فوراً: أي مجموعة حذفتها من الجدول ستطير من هنا
                chats_to_broadcast = [cid for cid in chats_to_broadcast if cid in db_active_ids]
            except Exception as e:
                logging.error(f"⚠️ فشل الرادار في تحديث القائمة من سوبابيس: {e}")

            # 🛑 [ مكبح الطوارئ ]
            # إذا أفرغت الجدول يدوياً أو انسحب الجميع، يغلق المحرك فوراً
            if not chats_to_broadcast:
                logging.info("🛑 [رادار]: جدول المشاركين فارغ في سوبابيس.. إنهاء المحرك.")
                if current_quiz_db_id:
                    supabase.table("active_quizzes").update({"is_active": False}).eq("id", current_quiz_db_id).execute()
                break 

            # 2️⃣ [ تجهيز البيانات اللحظية للسؤال ]
            answered_users_global[i + 1] = [] 
            ans = str(q.get('correct_answer') or q.get('answer_text') or "").strip()
            
            if is_bot:
                cat_name = q.get('category') or "بوت"
            else:
                cat_name = q['categories']['name'] if (q.get('categories') and isinstance(q['categories'], dict)) else "عام"
            
            # 💡 [ القاموس الموحد: المرجع الأساسي للمزامنة ]
            sync_data = {
                "is_active": True,
                "is_paused": False,
                "current_answer": ans,
                "current_index": i + 1,
                "total_questions": total_q,
                "question_category_name": cat_name,
                "quiz_style": current_style,
                "question_finished": False,
                "hint_sent": False,
                "votes_results": {"0": 0, "1": 0, "2": 0, "3": 0},
                "voter_list": [],
                "user_choices": {},
                "participants_ids": chats_to_broadcast # تحديث القائمة الصافية في سوبابيس
            }

            # 3️⃣ [ تحديث الرام للمجموعات الصامدة فقط ]
            for cid in chats_to_broadcast:
                # نحدث الرام المحلي لضمان أن المايسترو وقالب الإجابة يقرؤون أحدث البيانات
                active_quizzes[cid] = sync_data.copy()
                active_quizzes[cid].update({
                    "active": True,
                    "ans": ans,
                    "winners": [],
                    "voted_users": [], 
                    "mode": quiz_data.get('mode', 'السرعة ⚡'),
                    "quiz_id": current_quiz_db_id,
                    "category": cat_name,
                    "raw_q_data": q,
                    "quiz_type": "public"
                })

            # 4️⃣ [ مزامنة السحاب النهائية للسؤال الحالي ]
            if current_quiz_db_id:
                try:
                    supabase.table("active_quizzes").update(sync_data).eq("id", current_quiz_db_id).execute()
                except Exception as up_err:
                    logging.error(f"⚠️ فشل تحديث سجل سوبابيس: {up_err}")
        
            # --- [ تجهيز التلميح الذكي ] ---
            normal_hint_str = ""
            is_hint_on = quiz_data.get('smart_hint', False)
            if is_hint_on:
                ans_str = str(ans).strip()
                normal_hint_str = f"مكونة من ({len(ans_str.split())}) كلمات، تبدأ بـ ( {ans_str[0]} )"
                
            # 5️⃣ [ بث السؤال للعالم عبر المايسترو ]
            send_tasks = []
            for cid in chats_to_broadcast:
                task = send_quiz_master(
                    chat_id=cid, 
                    q_data=q, 
                    current_num=i + 1, 
                    total_num=total_q, 
                    settings={
                        'owner_name': owner_name,
                        'mode': quiz_data.get('mode', 'السرعة ⚡'),
                        'time_limit': int(quiz_data.get('time_limit', 15)),
                        'cat_name': cat_name,
                        'quiz_db_id': current_quiz_db_id,
                        'quiz_style': current_style,
                        'is_public': True
                    },
                    all_questions_list=selected_questions
                )
                send_tasks.append(task)

            # تنفيذ البث الجماعي (إرسال متوازي لسرعة فائقة)
            q_msgs = await asyncio.gather(*send_tasks, return_exceptions=True)

            # تسجيل معرفات الرسائل للتحكم بها (مسح أو إغلاق Poll)
            for idx, m in enumerate(q_msgs):
                if idx < len(chats_to_broadcast):
                    cid = chats_to_broadcast[idx]
                    if isinstance(m, types.Message):
                        messages_to_delete[cid].append(m.message_id)
                        if hasattr(m, 'poll') and m.poll is not None:
                            active_quizzes[cid]['last_poll_id'] = m.message_id  

            # 5️⃣ [ محرك الانتظار الموحد ]
            # 5️⃣ [ محرك الانتظار للنصوص المباشرة - وضع السرعة ] ⚡
            t_limit = int(quiz_data.get('time_limit', 15))
            start_wait = time.time()
            
            # حلقة مراقبة "نبض الرام"
            while time.time() - start_wait < t_limit:
                
                # 🛑 1. فحص الإيقاف الكلي (سوبابيس)
                if int(time.time() - start_wait) % 2 == 0:
                    try:
                        check = supabase.table("active_quizzes").select("is_active").eq("id", current_quiz_db_id).execute()
                        if check.data and not check.data[0].get('is_active', True):
                            break
                    except: pass

                # 🛑 2. فحص المجموعات النشطة في الرام
                active_chats = [c for c in chats_to_broadcast if c in active_quizzes]
                if not active_chats: break
                
                # 🔥 [ 3. المكبح النصي اللحظي ] 🔥
                # في وضع السرعة، بمجرد أن يكتب أي لاعب الإجابة الصحيحة في أي مجموعة
                # يقوم رادار الرصد بتغيير 'question_finished' إلى True
                is_answered = any(active_quizzes.get(c, {}).get('question_finished') for c in active_chats)
                
                if is_answered:
                    logging.info("⚡ [سرعة نصية]: بطل حسمها بالكتابة! كسر حلقة الانتظار.")
                    break # 👈 الهروب فوراً من الـ 15 ثانية وعرض النتائج
                
                # حساسية فائقة (0.1 ثانية) لالتقاط الكلمة فور وصولها للرام
                await asyncio.sleep(0.08)

            # ✅ تأكيد إغلاق السؤال عند الجميع برمجياً
            for cid in active_chats:
                if cid in active_quizzes:
                    active_quizzes[cid]['question_finished'] = True
                    active_quizzes[cid]['active'] = False # إيقاف استقبال إجابات إضافية
  
            # 🛑 [ الحركة القاضية - إغلاق الاستطلاعات ]
            # نغلق فقط في المجموعات التي لا تزال "موجودة" في الرام ولم تنسحب
            if current_style == 'اختيارات 📊':
                close_tasks = []
                for cid in chats_to_broadcast:
                    # لا نغلق البول إلا إذا كانت المجموعة لا تزال في الرام (لم تنسحب)
                    p_id = active_quizzes.get(cid, {}).get('last_poll_id')
                    if p_id:
                        try:
                            close_tasks.append(bot.stop_poll(cid, p_id))
                        except: pass
                
                if close_tasks:
                    # تنفيذ الإغلاق الجماعي بسرعة البرق
                    await asyncio.gather(*close_tasks, return_exceptions=True)                                       

            # 6️⃣ إغلاق السؤال وتحديث النقاط
            global_winners = []
            global_losers = []  
            
            # 🔥 [ 1. إعادة إنعاش القائمة - اللحظة الحاسمة ] 🔥
            # نحدث قائمة المجموعات "الآن" من سوبابيس لضمان عدم نسيان أي مجموعة جاوبت
            try:
                p_refresh = supabase.table("quiz_participants").select("chat_id").eq("quiz_id", current_quiz_db_id).execute()
                current_active_ids = [p['chat_id'] for p in p_refresh.data]
            except:
                current_active_ids = [cid for cid in all_chats if cid in active_quizzes]

            # تجميع الفائزين والمخطئين (المعالجة)
            for cid in current_active_ids:
                if cid in active_quizzes:
                    for winner in active_quizzes[cid].get('winners', []):
                        winner['home_cid'] = cid  
                        w_id = winner.get('id')
                        w_name = winner.get('name') or "لاعب"
                        winner['user_link'] = f'<a href="tg://user?id={w_id}">{w_name}</a>'
                        global_winners.append(winner)
                    
                    for loser in active_quizzes[cid].get('losers', []):
                        loser['home_cid'] = cid  
                        l_id = loser.get('id')
                        l_name = loser.get('name') or "لاعب"
                        loser['user_link'] = f'<a href="tg://user?id={l_id}">{l_name}</a>'
                        global_losers.append(loser)

            global_winners = sorted(global_winners, key=lambda x: x.get('time', 0))
            
            # 2️⃣ تحديث السجلات وتأمين الرام
            for cid in current_active_ids:
                # 🛡️ تأمين: لو المجموعة في سوبابيس بس مش في الرام، رجعها للرام فوراً
                if cid not in active_quizzes:
                    active_quizzes[cid] = {"active": False, "question_finished": True, "winners": [], "losers": []}
                
                active_quizzes[cid]['active'] = False
                active_quizzes[cid]['question_finished'] = True 
                
                local_winners = active_quizzes[cid].get('winners', [])
                group_points_claimed = False 
                gname = group_names_map.get(str(cid)) or group_names_map.get(cid) or "مجموعة نشطة"

                for w in local_winners:
                    uid, uname = w['id'], w['name']
                    pts_earned = w.get('pts', 10)
                    if uid not in group_scores[cid]:
                        group_scores[cid][uid] = {"name": uname, "mention": w.get('user_link'), "points": 0}
                    
                    group_scores[cid][uid]['points'] += pts_earned
                    if not group_points_claimed:
                        final_group_pts = pts_earned + 5
                        if 'group_total_points' in locals(): group_total_points[cid] += final_group_pts
                        await update_group_stats(cid, gname, uid, uname, final_group_pts)
                        group_points_claimed = True 
                    else:
                        await update_group_stats(cid, gname, uid, uname, 0)
                            
            # 3️⃣ [ بث القالب الملكي - تزامن شامل ] 🔥
            res_tasks = []
            for cid in current_active_ids:
                res_tasks.append(send_creative_results(
                    chat_id=cid, 
                    correct_ans=ans, 
                    winners=global_winners,  
                    losers=global_losers,    
                    group_scores=group_scores.get(cid, {}), 
                    is_public=True,
                    mode=quiz_data.get('mode', 'السرعة ⚡'),
                    group_names=group_names_map
                ))
            
            # إرسال متوازي لضمان السرعة لكل المجموعات في نفس اللحظة
            await asyncio.gather(*res_tasks, return_exceptions=True)
            
            # 7️⃣ العداد التنازلي المطور
            if i < total_q - 1:
                # مكبح الطوارئ من سوبابيس
                try:
                    check_stop = supabase.table("active_quizzes").select("is_active").eq("id", current_quiz_db_id).execute()
                    if check_stop.data and not check_stop.data[0].get('is_active', True): break
                except: pass

                # الفلترة النهائية للعداد (من بقي في سوبابيس)
                final_active_for_timer = [cid for cid in current_active_ids if cid in active_quizzes]
                if not final_active_for_timer: break

                for cid in final_active_for_timer:
                    # 🧼 [ تطهير القفل للجولة القادمة ] - أهم سطر
                    active_quizzes[cid]['winners'] = []
                    active_quizzes[cid]['losers'] = []
                    active_quizzes[cid]['question_finished'] = False # فتح القفل
                    active_quizzes[cid]['active'] = True # تفعيل الاستقبال

                # إطلاق العدادات الموحدة
                count_tasks = [run_countdown(cid) for cid in final_active_for_timer]
                await asyncio.gather(*count_tasks, return_exceptions=True)                                                    
            else:
                await asyncio.sleep(0.8)


        # 🏁 8️⃣ النتائج النهائية والتنظيف الرقمي المبرد ❄️
        
        async def global_cleanup_worker(q_id, chats, scores, cat):
            try:
                # 1. إرسال النتائج النهائية (الترتيب العالمي والمحلي)
                for cid in chats:
                    try:
                        await send_broadcast_final_results(
                            chat_id=cid, 
                            scores=scores, 
                            total_q=total_q, 
                            group_names=group_names_map
                        )
                        await asyncio.sleep(0.1) 
                    except: continue

                # 2. المزامنة النهائية مع سوبابيس (مرة واحدة للمسابقة كاملة)
                if q_id:
                    loop = asyncio.get_event_loop()
                    # ترحيل النقاط للجدول العالمي
                    await sync_points_to_global_db(group_scores=scores, quiz_id=q_id, cat_name=cat)
                    
                    # 🗑️ مسح سجل المسابقة النشطة من سوبابيس
                    await loop.run_in_executor(None, lambda: (
                        supabase.table("active_quizzes").delete().eq("id", q_id).execute()
                    ))
                    logging.info(f"✅ تم ترحيل وتنظيف بيانات المسابقة {q_id} من القاعدة.")

                # 3. 🔥 [ محرقة الرسائل والاستفتاءات ] 🔥
                for cid in chats:
                    # تجميع كل الرسائل: (أسئلة + نتائج فرعية + استفتاءات)
                    all_mids = list(set(messages_to_delete.get(cid, []) + results_to_delete.get(cid, [])))
                    
                    for idx, mid in enumerate(all_mids):
                        try: 
                            # محاولة حذف الرسالة (سواء كانت نصية أو Poll)
                            await bot.delete_message(cid, mid)
                            
                            # موازن السرعة لمنع الحظر (Flood)
                            if (idx + 1) % 10 == 0: 
                                await asyncio.sleep(1)
                        except Exception as e:
                            # إذا فشل الحذف (مثلاً الرسالة محذوفة أصلاً)، نتخطى بهدوء
                            pass

            except Exception as e:
                logging.error(f"🚨 Background Global Cleanup Error: {e}")

        # 🚀 إطلاق مهمة التنظيف في الخلفية لكي لا ينتظر المستخدم
        asyncio.create_task(global_cleanup_worker(
            current_quiz_db_id, 
            all_chats, 
            group_scores, 
            cat_name
        ))

    except Exception as e:
        logging.error(f"🚨 Global Engine Fatal Error: {e}")
    
    finally:
        for cid in all_chats: 
            active_broadcasts.discard(cid)
            if cid in active_quizzes: del active_quizzes[cid]
            # استخدام get لتجنب الخطأ إذا لم يكن موجوداً
            if cid in overall_scores: 
                overall_scores.pop(cid, None)
        logging.info("✨ تم تنظيف الذاكرة بالكامل.. نظام الإذاعة في وضع الاستعداد.")
      

# =======================================
# --- [ بداية الدالة من العمود 0 لضمان عدم وجود SyntaxError ] ---
import re
import difflib

def is_answer_correct(user_msg, correct_ans):
    """
    محرك رصد الإجابات الذكي (ياسر المطور - النسخة الشاملة للأرقام واللغة)
    """

    if not user_msg or not correct_ans: 
        return False

    # 1. قاموس الأرقام (قيم عددية للعمليات الحسابية)
    num_map = {
        "واحد": 1, "واحده": 1, "احد": 1, "اثنان": 2, "اثنين": 2,
        "ثلاثه": 3, "اربع": 4, "خمسه": 5, "سته": 6, "سبعه": 7,
        "ثمانيه": 8, "تسعه": 9, "عشره": 10, "عشرين": 20, "عشرون": 20,
        "ثلاثين": 30, "ثلاثون": 30, "اربعين": 40, "اربعون": 40,
        "خمسين": 50, "خمسون": 50, "مائه": 100, "الف": 1000
    }

    # 2. قاموس الحروف للتهجئة الصوتية
    char_map = {
        'a': 'ا', 'b': 'ب', 'c': 'ك', 'd': 'د', 'e': 'ا', 'f': 'ف', 'g': 'ج', 
        'h': 'ه', 'i': 'ي', 'j': 'ج', 'k': 'ك', 'l': 'ل', 'm': 'م', 'n': 'ن', 
        'o': 'و', 'p': 'ب', 'q': 'ق', 'r': 'ر', 's': 'س', 't': 'ت', 'u': 'و', 
        'v': 'ف', 'w': 'و', 'x': 'اكس', 'y': 'ي', 'z': 'ز'
    }

    stop_words = ["هو", "هي", "ال", "انه", "انها", "يكون", "يعتبر", "اسمها", "اسمه"]

    def clean_logic(text):
        """مرحلة التفكيك، المعالجة الرقمية، وإعادة البناء"""
        text = str(text).strip().lower()
        # إزالة التشكيل
        text = re.sub(r'[\u064B-\u0652]', '', text)
        
        # 1. تحويل الفرانكو/النطق الإنجليزي
        words = text.split()
        translated = []
        for w in words:
            if any(c.isascii() and c.isalpha() for c in w):
                w = "".join([char_map.get(c, c) for c in w])
            translated.append(w)
        
        text = " ".join(translated)
        # 2. توحيد الحروف الضعيفة والرموز
        text = re.sub(r'[أإآ]', 'ا', text)
        text = re.sub(r'ة', 'ه', text)
        text = re.sub(r'ى', 'ي', text)
        text = re.sub(r'[^\w\s]', ' ', text) # استبدال الرموز بمسافة للحفاظ على فصل الأرقام
        
        raw_words = text.split()
        processed_words = []
        
        # 3. محرك دمج الأرقام المركبة (مثل: اثنين وعشرين -> 22)
        i = 0
        while i < len(raw_words):
            w = raw_words[i]
            
            # حذف "ال" التعريف الزائدة
            if w.startswith("ال") and len(w) > 4: w = w[2:]
            
            # أ- معالجة الأرقام المفصولة بـ "و" (اثنين و عشرين)
            if i + 2 < len(raw_words) and raw_words[i+1] == "و":
                w1, w2 = w, raw_words[i+2]
                if w1 in num_map and w2 in num_map:
                    processed_words.append(str(num_map[w1] + num_map[w2]))
                    i += 3
                    continue
            
            # ب- معالجة الأرقام الملتصقة بالواو (اثنين وعشرين)
            if "و" in w and len(w) > 3:
                parts = w.split("و")
                # التأكد أن ما قبل وبعد الواو أرقام
                p1 = parts[0]
                p2 = parts[1]
                if p1 in num_map and p2 in num_map:
                    processed_words.append(str(num_map[p1] + num_map[p2]))
                    i += 1
                    continue
            
            # ج- تحويل الأرقام الفردية واستبعاد كلمات الحشو
            res = str(num_map.get(w, w))
            if res not in stop_words:
                processed_words.append(res)
            i += 1
            
        return processed_words

    user_words = clean_logic(user_msg)
    correct_words = clean_logic(correct_ans)

    # المرحلة 1: التطابق التام
    if " ".join(user_words) == " ".join(correct_words):
        return True

    # المرحلة 2: مطابقة الكلمات الذكية (SequenceMatcher للكلمات)
    for u in user_words:
        for c in correct_words:
            if u == c or difflib.SequenceMatcher(None, u, c).ratio() >= 0.85:
                return True

    # المرحلة 3: مطابقة الشظايا (Subwords) للأرشفة والالتصاق
    def subword_match(u_list, c_list):
        for u in u_list:
            if len(u) < 3: continue
            u_subs = [u[i:i+3] for i in range(len(u)-2)]
            for c in c_list:
                if len(c) < 3: continue
                c_subs = [c[i:i+3] for i in range(len(c)-2)]
                match_count = sum(1 for us in u_subs if any(
                    difflib.SequenceMatcher(None, us, cs).ratio() >= 0.85 for cs in c_subs
                ))
                if match_count / max(len(c_subs), 1) > 0.6:
                    return True
        return False

    if subword_match(user_words, correct_words):
        return True

    # المرحلة 4: نظام التشابه المرن للجملة الكاملة
    return difflib.SequenceMatcher(None, " ".join(user_words), " ".join(correct_words)).ratio() >= 0.80
# ==========================================
# 🎯 رادار الإجابات الموحد (نسخة ياسر النهائية)
# ==========================================
@dp.message_handler(lambda m: not m.text or not m.text.startswith('/'))
async def unified_answer_checker(m: types.Message):
    cid = m.chat.id
    uid = m.from_user.id
    user_text = m.text.strip() if m.text else ""

    # 1️⃣ فحص المسابقات النشطة
    if cid in active_quizzes and active_quizzes[cid].get('active'):
        quiz = active_quizzes[cid]

        # 🛑 [الخطوة 0: قفل نمط الأزرار] 🛑
        # إذا كان نمط المسابقة هو "اختيارات"، نوقف الرادار النصي فوراً
        # لكي لا يستطيع المستخدم كتابة الإجابة كتابةً
        if quiz.get('quiz_style') == 'اختيارات 📊':
            return # الخروج من الدالة وعدم الاستجابة للرسائل النصية

        correct_ans = str(quiz['ans']).strip()
        # ⚖️ فحص صحة الإجابة (هذا سيعمل فقط في النمط "مباشر" الآن)
        # ⚖️ فحص صحة الإجابة (هذا سيعمل فقط في النمط "مباشر" الآن)
        if is_answer_correct(user_text, correct_ans):
            
            # 🏁 [إضافة أثير المطور]: حساب وقت الاستجابة فوراً
            start_t = quiz.get('start_time')
            if not start_t:
                start_t = datetime.now() # حماية في حال فقدان الوقت
            
            response_time = (datetime.now() - start_t).total_seconds()
            t = float(response_time)

            # 🏆 حساب النقاط والألقاب (مثل نظام البول تماماً)
            if t < 3.0:
                s_title, extra_pts = "⚡ (خارق الصمت)", 100
            elif t < 4.0:
                s_title, extra_pts = "🚀 (القناص السريع)", 60
            elif t < 7.0:
                s_title, extra_pts = "🏹 (المتمكن)", 30
            else:
                s_title, extra_pts = "🧠 (الذكي)", 0
            
            total_pts = 10 + extra_pts # النقاط الأساسية + بونص السرعة

            # 🔥 [نظام منع التكرار العابر للمجموعات] 🔥
            p_ids = quiz.get('participants_ids', [cid])
            is_already_winner_globally = False
            
            for p_cid in p_ids:
                if p_cid in active_quizzes:
                    if any(w['id'] == uid for w in active_quizzes[p_cid].get('winners', [])):
                        is_already_winner_globally = True
                        break
            
            if is_already_winner_globally:
                logging.info(f"🚫 محاولة تكرار مرفوضة من {m.from_user.first_name}")
                return
            # 🛑 [نظام الإغلاق العالمي الفوري] ⚡ (وضع السرعة)
            # 🛑 [نظام الإغلاق العالمي الفوري] ⚡ (وضع السرعة)
            if quiz.get('mode') == 'السرعة ⚡':
                for p_cid in p_ids:
                    if p_cid in active_quizzes:
                        active_quizzes[p_cid]['question_finished'] = True # ✅ نغلق السؤال فقط
                
                logging.info(f"⚡ إغلاق عالمي: البطل {m.from_user.first_name} حسمها بلقب {s_title} في {t:.2f} ثانية.")

            # 💾 حفظ الإجابة في سوبابيس (استخدام total_pts و s_title)
            db_id = quiz.get('db_quiz_id')
            if db_id:
                def save_to_db():
                    try:
                        supabase.table("answers_log").insert({
                            "quiz_id": db_id,
                            "quiz_type": "public",
                            "question_no": quiz.get('current_index', 1),
                            "chat_id": cid, 
                            "group_name": m.chat.title,
                            "user_id": uid, 
                            "user_name": m.from_user.first_name,
                            "answer_text": f"{user_text} {s_title}", # إضافة اللقب للنص
                            "is_correct": True,
                            "points_earned": total_pts, # النقاط الجديدة
                            "response_time": t, # تسجيل الوقت للدقة
                            "speed_rank": len(quiz.get('winners', [])) + 1
                        }).execute()
                    except Exception as e: logging.error(f"❌ خطأ حفظ النتيجة: {e}")
                
                asyncio.create_task(asyncio.to_thread(save_to_db))

                # تسجيل الفائز مع بياناته الجديدة في الرام
                quiz['winners'].append({
                    "name": m.from_user.first_name, 
                    "id": uid, 
                    "pts": total_pts, 
                    "title": s_title,
                    "time": t
                })
                return


            else:
                # ==========================================
                # 🔒 مسار المسابقات الخاصة (نظام الإصلاح الشامل)
                # ==========================================
                # التأكد أن اللاعب لم يفز مسبقاً في هذا السؤال
                if not any(w['id'] == uid for w in quiz.get('winners', [])):
                    
                    # 🔹 جلب البيانات من الذاكرة النشطة (التي وضعناها في بداية المحرك)
                    db_quiz_id = quiz.get('quiz_id')    # الرقم المولد من سوبابيس
                    cat_name = quiz.get('category', 'عام') # اسم القسم (جغرافيا/تاريخ..)

                    # 💾 حفظ الإجابة في سوبابيس (جدول answers_log الجديد)
                    def save_private_to_db():
                        try:
                            supabase.table("answers_log").insert({
                                "quiz_id": db_quiz_id,          # ✅ تم التخلص من None
                                "category_name": cat_name,      # ✅ إضافة اسم القسم
                                "quiz_type": "private",
                                "question_no": quiz.get('current_index', 1),
                                "total_quiz_questions": quiz.get('total_questions', 1),
                                "chat_id": cid,
                                "group_name": m.chat.title or "مسابقة خاصة",
                                "user_id": uid,
                                "user_name": m.from_user.first_name,
                                "answer_text": user_text,
                                "is_correct": True,
                                "points_earned": 2,
                                "speed_rank": len(quiz.get('winners', [])) + 1
                            }).execute()
                        except Exception as e: 
                            logging.error(f"❌ خطأ حفظ النتيجة في الجدول الجديد: {e}")
                    
                    # تنفيذ الحفظ في خلفية البوت لضمان السرعة
                    asyncio.create_task(asyncio.to_thread(save_private_to_db))

                    # تسجيل الفائز في ذاكرة المسابقة الحالية
                    quiz.setdefault('winners', []).append({"name": m.from_user.first_name, "id": uid})
                    
                    # إذا كان النمط "سرعة"، نوقف السؤال فور أول إجابة صحيحة
                    if quiz.get('mode') == 'السرعة ⚡':
                        quiz['question_finished'] = True# علامة لإنهاء السؤال فقط
                    return

# ==========================================
# --- [ رادار إجابات الـ Poll الموحد مع سوبابيس ] ---
# ==========================================
@dp.poll_answer_handler()
async def handle_poll_answer(poll_answer: types.PollAnswer):
    user_id = poll_answer.user.id
    user_name = poll_answer.user.full_name
    poll_id = poll_answer.poll_id
    
    # 1. جلب بيانات السؤال من المخزن السريع (active_polls)
    poll_info = active_polls.get(poll_id)
    if not poll_info:
        return

    cid = poll_info.get('chat_id')
    db_quiz_id = poll_info.get('db_quiz_id') # هذا هو ID المسابقة في سوبابيس
    q_index = poll_info.get('current_num')

    # 🛑 [ حماية الغش: منع الإجابة المتعددة ]
    if q_index in answered_users_global and user_id in answered_users_global[q_index]:
        print(f"🚫 [محاولة غش]: {user_name} حاول الإجابة مرة أخرى!")
        return

    # 2. التحقق من الإجابة وحساب الوقت
    user_option_id = poll_answer.option_ids[0] 
    is_correct = (user_option_id == poll_info['correct_id'])
    response_time = (datetime.now() - poll_info['start_time']).total_seconds()
    t = float(response_time)

    # 3. تسجيل في القائمة العالمية فوراً
    if q_index not in answered_users_global:
        answered_users_global[q_index] = []
    answered_users_global[q_index].append(user_id)

    # 4. حساب النقاط والألقاب
    if is_correct:
        if t < 2.5: s_title, extra_pts = "⚡ (خارق الصمت)", 100
        elif t < 4.0: s_title, extra_pts = "🚀 (القناص السريع)", 60
        elif t < 6.0: s_title, extra_pts = "🏹 (المتمكن)", 30
        else: s_title, extra_pts = "🧠 (الذكي)", 0
        total_pts = 10 + extra_pts
    else:
        s_title, total_pts = "", 0

    # 5. [ الربط الموحد مع الرام وسوبابيس ] 🔥
    if cid in active_quizzes:
        # أ: تحديث الرام المحلي (الموحد)
        if is_correct:
            winner_entry = {
                'id': user_id,
                'name': user_name,
                'time': round(t, 3),
                'title': s_title,
                'pts': total_pts
            }
            active_quizzes[cid]['winners'].append(winner_entry)
            
            # ب: مزامنة قائمة الفائزين مع سوبابيس (voter_list)
            # نرسل القائمة كاملة للسحاب ليراها المشرف لحظياً
            try:
                if db_quiz_id:
                    supabase.table("active_quizzes").update({
                        "voter_list": active_quizzes[cid]['winners'],
                        "user_choices": {str(user_id): user_option_id}
                    }).eq("id", db_quiz_id).execute()
            except Exception as e:
                logging.error(f"⚠️ فشل تحديث رادار سوبابيس: {e}")
        else:
            # تسجيل الخاسرين محلياً (التشطيب الملكي)
            if 'losers' not in active_quizzes[cid]:
                active_quizzes[cid]['losers'] = []
            active_quizzes[cid]['losers'].append({'id': user_id, 'name': user_name, 'penalty': 5})

    # 6. تجهيز بيانات الإدراج لجدول answers_log (السجل التاريخي)
    answer_data = {
        "quiz_id": db_quiz_id,
        "quiz_type": poll_info.get('quiz_type', 'private'), 
        "quiz_style": "اختيارات 📊",
        "category_name": poll_info.get('category', 'عام'),
        "chat_id": cid,
        "user_id": user_id,
        "user_name": user_name,
        "is_correct": is_correct,
        "points_earned": total_pts,
        "question_no": q_index,
        "total_quiz_questions": poll_info.get('total_num'),    
        "answer_text": poll_info.get('correct_text') if is_correct else "إجابة خاطئة",
        "response_time": round(t, 3),
        "created_at": "now()"
    }

    # 🚀 تسجيل العملية في سجل الإجابات
    await record_poll_answer_in_db(answer_data)

    # 📡 طباعة للرصد
    status = f"✅ كفو (+{total_pts}ن)" if is_correct else "❌ أخطأ (-5ن)"
    print(f"📡 [رصد]: {user_name} | {status} | الوقت: {t:.2f}ث")

# ============================================================
# 1. إعداد حالات الإدارة - Admin States
# ============================================================
class AdminStates(StatesGroup):
    waiting_for_new_token = State()      
    waiting_for_broadcast = State()      
    waiting_for_broadcast_photo = State()
    waiting_for_key_value = State() 
    # حالات إدارة المتغيرات (المجموعات والمستودعات)
    waiting_for_var_name = State()   # لاسم المتغير الجديد
    waiting_for_var_value = State()  # لقيمة الـ ID الجديد


# =========================================
# 2. كيبوردات غرفة عمليات المطور
# =========================================
def get_main_admin_kb():
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("📊 إدارة الأسئلة", callback_data="botq_main"),
        InlineKeyboardButton("📝 مراجعة الطلبات", callback_data="admin_view_pending"),
        InlineKeyboardButton("📢 إذاعة عامة", callback_data="admin_broadcast"),
        InlineKeyboardButton("🔄 تحديث النظام", callback_data="admin_restart_now"),
        InlineKeyboardButton("🔑 مفاتيح GROQ", callback_data="admin_keys_hub"),
        # الزر الجديد لإدارة المجموعات (المستودعات)
        InlineKeyboardButton("📦 متغيرات النظام", callback_data="manage_vars_main") 
    )
    kb.row(InlineKeyboardButton("🔐 استبدال توكين البوت", callback_data="admin_change_token"))
    kb.row(InlineKeyboardButton("❌ إغلاق اللوحة", callback_data="botq_close"))
    return kb

# كيبورد إدارة متغيرات النظام (ديناميكي من سوبابيس)
async def get_vars_management_kb():
    kb = InlineKeyboardMarkup(row_width=1)
    # جلب المتغيرات المسجلة في جدول bot_variables
    res = supabase.table("bot_variables").select("*").execute()
    
    for var in res.data:
        kb.add(InlineKeyboardButton(
            f"⚙️ {var['var_name']}: {var['var_value']}", 
            callback_data=f"edit_var_{var['var_name']}"
        ))
    
    kb.add(InlineKeyboardButton("➕ إضافة متغير (مخزن) جديد", callback_data="add_new_var"))
    kb.add(InlineKeyboardButton("🔙 رجوع", callback_data="admin_back"))
    return kb

# ============================================================
# 3. معالجات إدارة متغيرات النظام (المستودعات والسجلات)
# ============================================================

# دخول لوحة إدارة المتغيرات
@dp.callback_query_handler(lambda c: c.data == "manage_vars_main", user_id=ADMIN_ID)
async def admin_manage_vars_hub(c: types.CallbackQuery):
    reply_markup = await get_vars_management_kb()
    txt = (
        "📦 <b>إدارة مستودعات ومتغيرات النظام</b>\n"
        "━━━━━━━━━━━━━━\n"
        "هذه المجموعات تعمل كمخازن وسجلات للبوت.\n"
        "يمكنك تحديث الـ ID أو إضافة مخزن جديد بالكامل."
    )
    await c.message.edit_text(txt, reply_markup=reply_markup, parse_mode="HTML")

# --- أ: تحديث قيمة متغير موجود ---
@dp.callback_query_handler(lambda c: c.data.startswith("edit_var_"), user_id=ADMIN_ID)
async def ask_to_edit_var(c: types.CallbackQuery, state: FSMContext):
    var_name = c.data.replace("edit_var_", "")
    await AdminStates.waiting_for_var_value.set()
    await state.update_data(target_var=var_name, mode="update") # وضع التحديث
    
    await c.message.edit_text(
        f"🔄 <b>تحديث المعرف لـ:</b> <code>{var_name}</code>\n"
        "أرسل الـ ID الجديد للمجموعة الآن (يبدأ بـ -100):",
        parse_mode="HTML"
    )

# --- ب: إضافة متغير جديد تماماً ---
@dp.callback_query_handler(text="add_new_var", user_id=ADMIN_ID)
async def start_add_var(c: types.CallbackQuery):
    await AdminStates.waiting_for_var_name.set()
    await c.message.edit_text(
        "✨ <b>إضافة مخزن/متغير جديد</b>\n"
        "أرسل اسم المتغير بالإنجليزي (مثال: <code>GALLERY_GROUP</code>):",
        parse_mode="HTML"
    )

@dp.message_handler(state=AdminStates.waiting_for_var_name, user_id=ADMIN_ID)
async def get_var_name_to_add(message: types.Message, state: FSMContext):
    var_name = message.text.strip().upper().replace(" ", "_")
    await state.update_data(target_var=var_name, mode="insert") # وضع الإضافة
    await AdminStates.waiting_for_var_value.set()
    await message.answer(f"✅ تم اعتماد الاسم: <code>{var_name}</code>\nأرسل الآن الـ ID الخاص بالمجموعة:")

# --- ج: الحفظ النهائي (للتحديث أو الإضافة) ---
@dp.message_handler(state=AdminStates.waiting_for_var_value, user_id=ADMIN_ID)
async def save_var_to_supabase(message: types.Message, state: FSMContext):
    try:
        new_id = int(message.text)
        data = await state.get_data()
        var_name = data.get("target_var")
        mode = data.get("mode")

        if mode == "update":
            supabase.table("bot_variables").update({"var_value": new_id}).eq("var_name", var_name).execute()
            await message.answer(f"✅ تم تحديث <code>{var_name}</code> بنجاح!", parse_mode="HTML")
        else:
            supabase.table("bot_variables").insert({
                "var_name": var_name, 
                "var_value": new_id, 
                "description": "تمت الإضافة من البوت"
            }).execute()
            await message.answer(f"🚀 تم إنشاء المتغير <code>{var_name}</code> بنجاح!", parse_mode="HTML")
            
        await state.finish()
    except Exception as e:
        await message.answer(f"❌ حدث خطأ: يرجى التأكد من إرسال رقم ID صحيح.\n{e}")

# ============================================================
# 4. المعالجات العامة (لوحتي، إغلاق، رجوع)
# ============================================================

@dp.message_handler(commands=['admin'], user_id=ADMIN_ID)
@dp.message_handler(lambda m: m.text in ['لوحتي', 'المطور', 'غرفة العمليات'], user_id=ADMIN_ID)
async def admin_dashboard(message: types.Message):
    try:
        res = supabase.table("groups_hub").select("*").execute()
        active = len([g for g in res.data if g['status'] == 'active'])
        total_points = sum([g.get('total_group_score', 0) for g in res.data])

        txt = (
            "👑 <b>غرفة العمليات الرئيسية</b>\n"
            "━━━━━━━━━━━━━━\n"
            f"✅ المجموعات النشطة: <b>{active}</b>\n"
            f"🏆 إجمالي نقاط الهب: <b>{total_points:,}</b>\n"
            "━━━━━━━━━━━━━━\n"
            "👇 اختر قسماً لإدارته:"
        )
        await message.answer(txt, reply_markup=get_main_admin_kb(), parse_mode="HTML")
    except:
        await message.answer("❌ خطأ في الاتصال بقاعدة البيانات.")

@dp.callback_query_handler(lambda c: c.data == "admin_back", user_id=ADMIN_ID, state="*")
async def admin_back_to_main(c: types.CallbackQuery, state: FSMContext):
    await state.finish()
    # إعادة استدعاء اللوحة الرئيسية
    await admin_dashboard(c.message)
    await c.message.delete() # حذف الرسالة القديمة لتجنب التكرار

@dp.callback_query_handler(text="botq_close", user_id=ADMIN_ID)
async def close_admin_panel(c: types.CallbackQuery):
    await c.message.delete()
    await c.answer("تم إغلاق غرفة العمليات.")
# --- 1. معالج الأمر الرئيسي /admin (المعدل للنظام الموحد) ---
@dp.message_handler(commands=['admin'], user_id=ADMIN_ID)
@dp.message_handler(lambda m: m.text in ['لوحتي', 'المطور', 'غرفة العمليات'], user_id=ADMIN_ID)
async def admin_dashboard(message: types.Message):
    try:
        res = supabase.table("groups_hub").select("*").execute()
        active = len([g for g in res.data if g['status'] == 'active'])
        blocked = len([g for g in res.data if g['status'] == 'blocked'])
        total_global_points = sum([g.get('total_group_score', 0) for g in res.data])

        txt = (
            "👑 <b>غرفة العمليات الرئيسية</b>\n"
            "━━━━━━━━━━━━━━\n"
            f"✅ المجموعات النشطة: <b>{active}</b>\n"
            f"🚫 المجموعات المحظورة: <b>{blocked}</b>\n"
            f"🏆 إجمالي نقاط الهب: <b>{total_global_points:,}</b>\n"
            "━━━━━━━━━━━━━━\n"
            "👇 اختر قسماً لإدارته:"
        )
        await message.answer(txt, reply_markup=get_main_admin_kb(), parse_mode="HTML")
    except Exception as e:
        logging.error(f"Admin Panel Error: {e}")
        await message.answer("❌ خطأ في الاتصال بقاعدة البيانات الموحدة.")
# --- 2. معالج العودة للقائمة الرئيسية (المعدل) ---
@dp.callback_query_handler(lambda c: c.data == "admin_back", user_id=ADMIN_ID, state="*")
async def admin_back_to_main(c: types.CallbackQuery, state: FSMContext):
    await state.finish()
    try:
        res = supabase.table("groups_hub").select("*").execute()
        active = len([g for g in res.data if g['status'] == 'active'])
        blocked = len([g for g in res.data if g['status'] == 'blocked'])
        total_global_points = sum([g.get('total_group_score', 0) for g in res.data])
        
        txt = (
            "👑 <b>غرفة العمليات الرئيسية</b>\n"
            "━━━━━━━━━━━━━━\n"
            f"✅ المجموعات النشطة: <b>{active}</b>\n"
            f"🚫 المجموعات المحظورة: <b>{blocked}</b>\n"
            f"🏆 إجمالي نقاط الهب: <b>{total_global_points:,}</b>\n"
            "━━━━━━━━━━━━━━"
        )
        await c.message.edit_text(txt, reply_markup=get_main_admin_kb(), parse_mode="HTML")
    except Exception as e:
        await c.answer("⚠️ حدث خطأ أثناء تحديث البيانات الموحدة")

# --- 3. قسم إدارة مفاتيح GROQ (التحديث الجديد) ---

@dp.callback_query_handler(text="admin_keys_hub", user_id=ADMIN_ID)
async def show_keys_hub(c: types.CallbackQuery):
    txt = (
        "🔑 <b>إدارة مفاتيح GROQ الاحتياطية</b>\n"
        "━━━━━━━━━━━━━━\n"
        "اختر المفتاح المراد تفعيله للعمل حالياً، أو قم بتحديث مفتاح موجود:"
    )
    await c.message.edit_text(txt, reply_markup=get_keys_management_kb(), parse_mode="HTML")

@dp.callback_query_handler(text="admin_update_any_key", user_id=ADMIN_ID)
async def start_key_update(c: types.CallbackQuery):
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(
        InlineKeyboardButton("🔑 تحديث G_KEY_1", callback_data="target_G_KEY_1"),
        InlineKeyboardButton("🔑 تحديث G_KEY_2", callback_data="target_G_KEY_2"),
        InlineKeyboardButton("🔑 تحديث G_KEY_3", callback_data="target_G_KEY_3"),
        InlineKeyboardButton("🔙 إلغاء", callback_data="admin_keys_hub")
    )
    await c.message.edit_text("🎯 <b>اختر الرقم الذي تريد حفظ المفتاح الجديد فيه:</b>", reply_markup=kb, parse_mode="HTML")

@dp.callback_query_handler(lambda c: c.data.startswith("target_"), user_id=ADMIN_ID)
async def set_target_key(c: types.CallbackQuery, state: FSMContext):
    target = c.data.replace("target_", "")
    await state.update_data(selected_key_name=target)
    await AdminStates.waiting_for_new_token.set() 
    await c.message.answer(f"📥 <b>أرسل الآن مفتاح GROQ الجديد:</b>\nسيتم حفظه في: <code>{target}</code>", parse_mode="HTML")
    await c.answer()

@dp.message_handler(state=AdminStates.waiting_for_new_token, user_id=ADMIN_ID)
async def save_key_to_db(message: types.Message, state: FSMContext):
    new_token = message.text.strip()
    user_data = await state.get_data()
    target_key_name = user_data.get("selected_key_name")

    if not new_token.startswith("gsk_"):
        await message.answer("⚠️ يبدو أن هذا ليس مفتاح Groq صالح. حاول مرة أخرى.")
        return

    try:
        # تحديث السجل المختار والمفتاح النشط فوراً
        supabase.table("system_settings").update({"key_value": new_token}).eq("key_name", target_key_name).execute()
        supabase.table("system_settings").update({"key_value": new_token}).eq("key_name", "ACTIVE_GROQ_KEY").execute()

        await message.answer(f"✅ <b>تم التحديث والتفعيل بنجاح!</b>\n📍 الموقع: <code>{target_key_name}</code>", parse_mode="HTML")
        await state.finish()
        await admin_dashboard(message) 
    except Exception as e:
        await message.answer(f"❌ خطأ في السوبابيس: {e}")
        await state.finish()
# ============================================================
# --- [ معالج تفعيل (تبديل) المفتاح النشط ] ---
# ============================================================

@dp.callback_query_handler(lambda c: c.data.startswith("gkey_"), user_id=ADMIN_ID)
async def activate_key_by_slot(c: types.CallbackQuery):
    """
    هذا المعالج يقرأ القيمة المخزنة في G_KEY_1 أو 2 أو 3 
    ويقوم بنسخها إلى ACTIVE_GROQ_KEY ليعمل بها البوت فوراً.
    """
    selected_slot = c.data.replace("gkey_", "") # استخراج اسم السلوت
    
    try:
        # 1. جلب القيمة من السجل المختار
        res = supabase.table("system_settings").select("key_value").eq("key_name", selected_slot).execute()
        
        if res.data and res.data[0]['key_value']:
            target_token = res.data[0]['key_value']
            
            # 2. تحديث سجل ACTIVE_GROQ_KEY ليكون هو المحرك الحالي
            supabase.table("system_settings").update({
                "key_value": target_token,
                "description": f"Currently active key from {selected_slot}"
            }).eq("key_name", "ACTIVE_GROQ_KEY").execute()
            
            # 3. إشعار المطور بنجاح التبديل
            await c.answer(f"🚀 تم تفعيل {selected_slot} بنجاح!", show_alert=True)
            
            # تحديث نص الرسالة لإظهار المفتاح الحالي
            new_txt = (
                f"✅ <b>تم تغيير المحرك بنجاح!</b>\n"
                f"━━━━━━━━━━━━━━\n"
                f"المفتاح النشط الآن: <code>{selected_slot}</code>\n"
                f"تم سحب البيانات من جدول <code>system_settings</code>."
            )
            await c.message.edit_text(new_txt, reply_markup=get_keys_management_kb(), parse_mode="HTML")
        
        else:
            await c.answer(f"❌ خطأ: سجل {selected_slot} فارغ، قم بتحديثه أولاً.", show_alert=True)

    except Exception as e:
        logging.error(f"Activation Error: {e}")
        await c.answer("⚠️ فشل الاتصال بقاعدة البيانات لتفعيل المفتاح.", show_alert=True)
# =========================================
# --- 3. معالج زر التحديث (Restart) ---
@dp.callback_query_handler(text="admin_restart_now", user_id=ADMIN_ID)
async def system_restart(c: types.CallbackQuery):
    await c.message.edit_text("🔄 <b>جاري تحديث النظام وإعادة التشغيل...</b>", parse_mode="HTML")
    await bot.close()
    await storage.close()
    os._exit(0)
# --- 4. معالج زر استبدال التوكين ---
@dp.callback_query_handler(text="admin_change_token", user_id=ADMIN_ID)
async def ask_new_token(c: types.CallbackQuery):
    await c.message.edit_text(
        "📝 <b>أرسل التوكين الجديد الآن:</b>\n"
        "⚠️ سيتم الحفظ في Supabase وإعادة التشغيل فوراً.", 
        reply_markup=InlineKeyboardMarkup().add(InlineKeyboardButton("⬅️ تراجع", callback_data="admin_back"))
    )
    await AdminStates.waiting_for_new_token.set()

    # --- [ إدارة أسئلة البوت الرسمية - نسخة ياسر الملك المحدثة 2026 ] ---

@dp.callback_query_handler(lambda c: c.data.startswith('botq_'), user_id=ADMIN_ID)
async def process_bot_questions_panel(c: types.CallbackQuery, state: FSMContext):
    data_parts = c.data.split('_')
    action = data_parts[1]

    if action == "close":
        await c.message.delete()
        await c.answer("تم الإغلاق")

    elif action == "main":
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("📥 رفع (Bulk)", callback_data="botq_upload"),
            InlineKeyboardButton("📂 عرض المجلدات", callback_data="botq_viewfolders"),
            InlineKeyboardButton("⬅️ عودة للرئيسية", callback_data="admin_back")
        )
        await c.message.edit_text("🛠️ <b>إدارة الأسئلة (نظام المجلدات)</b>", reply_markup=kb, parse_mode="HTML")

    elif action == "upload":
        await c.message.edit_text(
            "📥 <b>وضع الرفع المطور:</b>\n\n"
            "أرسل الأسئلة بالصيغة التالية:\n"
            "<code>سؤال+إجابة+القسم+المجلد</code>\n\n"
            "أرسل <b>خروج</b> للعودة.", 
            parse_mode="HTML"
        )
        await state.set_state("wait_for_bulk_questions")

    # --- المستوى الأول: عرض المجلدات ---
    elif action == "viewfolders":
        res = supabase.table("folders").select("*").execute()
        if not res.data:
            return await c.answer("⚠️ لا توجد مجلدات مسجلة.", show_alert=True)
        
        kb = InlineKeyboardMarkup(row_width=2)
        for folder in res.data:
            kb.insert(InlineKeyboardButton(f"📁 {folder['name']}", callback_data=f"botq_showcats_{folder['id']}"))
        
        kb.add(InlineKeyboardButton("⬅️ عودة للرئيسية", callback_data="botq_main"))
        await c.message.edit_text("📂 <b>المجلدات الرئيسية:</b>\nاختر مجلداً لعرض أقسامه:", reply_markup=kb, parse_mode="HTML")

    # --- المستوى الثاني: عرض الأقسام داخل المجلد ---
    elif action == "showcats":
        folder_id = data_parts[2]
        res = supabase.table("bot_categories").select("*").eq("folder_id", folder_id).execute()
        
        kb = InlineKeyboardMarkup(row_width=2)
        if res.data:
            for cat in res.data:
                kb.insert(InlineKeyboardButton(f"🏷️ {cat['name']}", callback_data=f"botq_mng_{cat['id']}"))
        else:
            kb.add(InlineKeyboardButton("🚫 لا توجد أقسام هنا", callback_data="none"))
            
        kb.add(InlineKeyboardButton("🔙 عودة للمجلدات", callback_data="botq_viewfolders"))
        await c.message.edit_text("🗂️ <b>الأقسام المتوفرة في هذا المجلد:</b>", reply_markup=kb, parse_mode="HTML")

    # --- المستوى الثالث: إدارة القسم المختار ---
    elif action == "mng":
        cat_id = data_parts[2]
        res = supabase.table("bot_questions").select("id", count="exact").eq("bot_category_id", int(cat_id)).execute()
        q_count = res.count if res.count is not None else 0
        
        kb = InlineKeyboardMarkup(row_width=1)
        kb.add(
            InlineKeyboardButton(f"🗑️ حذف جميع الأسئلة ({q_count})", callback_data=f"botq_confdel_{cat_id}"),
            InlineKeyboardButton("🔙 عودة للأقسام", callback_data="botq_viewfolders")
        )
        await c.message.edit_text(
            f"📊 <b>إدارة القسم (ID: {cat_id})</b>\n\n"
            f"عدد الأسئلة المتوفرة: <b>{q_count}</b>\n\n"
            "⚠️ تنبيه: خيار الحذف سيقوم بمسح كافة الأسئلة التابعة لهذا القسم فقط.", 
            reply_markup=kb, parse_mode="HTML"
        )

    # --- نظام الحماية: تأكيد الحذف (نعم / لا) ---
    elif action == "confdel":
        cat_id = data_parts[2]
        kb = InlineKeyboardMarkup(row_width=2)
        kb.add(
            InlineKeyboardButton("✅ نعم، احذف", callback_data=f"botq_realdel_{cat_id}"),
            InlineKeyboardButton("❌ تراجع (إلغاء)", callback_data=f"botq_mng_{cat_id}")
        )
        await c.message.edit_text(
            "⚠️ <b>تأكيد الحذف النهائي!</b>\n\n"
            "هل أنت متأكد من مسح جميع أسئلة هذا القسم؟\n"
            "لا يمكن التراجع عن هذه العملية بعد التنفيذ.", 
            reply_markup=kb, parse_mode="HTML"
        )

    # تنفيذ الحذف الفعلي
    elif action == "realdel":
        cat_id = data_parts[2]
        try:
            supabase.table("bot_questions").delete().eq("bot_category_id", int(cat_id)).execute()
            await c.answer("✅ تم الحذف بنجاح", show_alert=True)
            await process_bot_questions_panel(c, state) # العودة للرئيسية
        except Exception as e:
            await c.answer(f"❌ خطأ: {e}", show_alert=True)

    await c.answer()

# --- معالج الرفع المطور (سؤال+إجابة+قسم+مجلد) ---
@dp.message_handler(state="wait_for_bulk_questions", user_id=ADMIN_ID)
async def process_bulk_questions(message: types.Message, state: FSMContext):
    if message.text.strip() in ["خروج", "إلغاء", "exit"]:
        await state.finish()
        await message.answer("✅ تم الخروج من وضع الرفع.")
        return

    lines = message.text.split('\n')
    success, error = 0, 0
    
    for line in lines:
        if '+' in line:
            parts = line.split('+')
            if len(parts) == 4:
                q_text, q_ans, cat_name, f_name = [p.strip() for p in parts]
                try:
                    # 1. فحص المجلد
                    f_res = supabase.table("folders").select("id").eq("name", f_name).execute()
                    f_id = f_res.data[0]['id'] if f_res.data else supabase.table("folders").insert({"name": f_name}).execute().data[0]['id']

                    # 2. فحص القسم وربطه
                    c_res = supabase.table("bot_categories").select("id").eq("name", cat_name).execute()
                    if c_res.data:
                        cat_id = c_res.data[0]['id']
                        supabase.table("bot_categories").update({"folder_id": f_id}).eq("id", cat_id).execute()
                    else:
                        cat_id = supabase.table("bot_categories").insert({"name": cat_name, "folder_id": f_id}).execute().data[0]['id']

                    # 3. رفع السؤال
                    supabase.table("bot_questions").insert({
                        "question_content": q_text,
                        "correct_answer": q_ans,
                        "bot_category_id": cat_id,
                        "category": cat_name,
                        "created_by": str(ADMIN_ID)
                    }).execute()
                    success += 1
                except Exception as e:
                    logging.error(f"Error: {e}")
                    error += 1
            else: error += 1
        elif line.strip(): error += 1

    await message.answer(
        f"📊 <b>ملخص الرفع النهائي (ياسر الملك):</b>\n"
        f"✅ نجاح: {success}\n"
        f"❌ فشل: {error}\n\n"
        f"📥 أرسل الدفعة التالية أو أرسل 'خروج'.", 
        parse_mode="HTML"
    )

# ==========================================
# إدارة مجموعات الهب (الموافقة، الحظر، التفعيل)
# ==========================================

# 1. قائمة المجموعات (عرض الحالات: انتظار ⏳، نشط ✅، محظور 🚫)
@dp.callback_query_handler(lambda c: c.data == "admin_view_pending", user_id=ADMIN_ID)
async def admin_manage_groups(c: types.CallbackQuery):
    try:
        res = supabase.table("groups_hub").select("group_id, group_name, status").execute()
        
        if not res.data:
            return await c.answer("📭 لا توجد مجموعات مسجلة بعد.", show_alert=True)
        
        txt = (
            "🛠️ <b>إدارة مجموعات الهب الموحد:</b>\n\n"
            "⏳ = بانتظار الموافقة (Pending)\n"
            "✅ = نشطة وشغالة (Active)\n"
            "🚫 = محظورة (Blocked)\n"
            "━━━━━━━━━━━━━━"
        )
        
        kb = InlineKeyboardMarkup(row_width=1)
        for g in res.data:
            status_icon = "⏳" if g['status'] == 'pending' else "✅" if g['status'] == 'active' else "🚫"
            
            kb.add(
                InlineKeyboardButton(
                    f"{status_icon} {g['group_name']}", 
                    callback_data=f"manage_grp_{g['group_id']}"
                )
            )
        
        kb.add(InlineKeyboardButton("⬅️ العودة للقائمة الرئيسية", callback_data="admin_back"))
        await c.message.edit_text(txt, reply_markup=kb, parse_mode="HTML")
    except Exception as e:
        logging.error(f"Error viewing groups: {e}")
        await c.answer("❌ خطأ في جلب البيانات")

# 2. لوحة التحكم بمجموعة محددة (إعطاء الصلاحية أو سحبها)
@dp.callback_query_handler(lambda c: c.data.startswith('manage_grp_'), user_id=ADMIN_ID)
async def group_control_options(c: types.CallbackQuery):
    g_id = c.data.split('_')[2]
    res = supabase.table("groups_hub").select("group_name, status").eq("group_id", g_id).execute()
    
    if not res.data: 
        return await c.answer("⚠️ المجموعة غير موجودة.")
    
    g = res.data[0]
    status_map = {'active': 'نشطة ✅', 'pending': 'بانتظار الموافقة ⏳', 'blocked': 'محظورة 🚫'}
    
    txt = (
        f"📍 <b>إدارة المجموعة:</b> {g['group_name']}\n"
        f"🆔 الآيدي: <code>{g_id}</code>\n"
        f"⚙️ الحالة الحالية: <b>{status_map.get(g['status'], g['status'])}</b>\n"
        f"━━━━━━━━━━━━━━"
    )

    kb = InlineKeyboardMarkup(row_width=2)
    if g['status'] != 'active':
        kb.add(InlineKeyboardButton("✅ موافقة وتفعيل", callback_data=f"auth_approve_{g_id}"))
    if g['status'] != 'blocked':
        kb.add(InlineKeyboardButton("🚫 رفض وحظر", callback_data=f"auth_block_{g_id}"))
    
    kb.add(InlineKeyboardButton("⬅️ رجوع للقائمة", callback_data="admin_view_pending"))
    await c.message.edit_text(txt, reply_markup=kb, parse_mode="HTML")
    
# ==========================================
# 7. معالج العمليات (Admin Callbacks)
# ==========================================
@dp.callback_query_handler(lambda c: c.data.startswith(('auth_approve_', 'auth_block_')), user_id=ADMIN_ID)
async def process_auth_callback(c: types.CallbackQuery):
    action = c.data.split('_')[1]
    target_id = c.data.split('_')[2]
    
    if action == "approve":
        supabase.table("groups_hub").update({"status": "active"}).eq("group_id", target_id).execute()
        await c.answer("تم تفعيل المجموعة بنجاح! ✅", show_alert=True)
        
        try:
            full_template = (
                f"🎉 <b>تم تفعيل القروب بنجاح!</b>\n"
                f"━━━━━━━━━━━━━━\n"
                f"⚙️ الحالة: متصل (Active) ✅\n"
                f"━━━━━━━━━━━━━━\n\n"
                f"🚀 <b>دليلك السريع للبدء:</b>\n"
                f"🔹 <b>تحكم :</b> لوحة الإعدادات ⚙️\n"
                f"🔹 <b>مسابقة :</b> لبدء التنافس 📝\n"
                f"🔹 <b>عني :</b> ملفك الشخصي ونقاطك 👤\n"
                f"🔹 <b>القروبات :</b> الترتيب العالمي 🌍\n\n"
                f"━━━━━━━━━━━━━━"
            )
            await bot.send_message(target_id, full_template, parse_mode="HTML")
        except: pass

    elif action == "block":
        supabase.table("groups_hub").update({"status": "blocked"}).eq("group_id", target_id).execute()
        await c.answer("تم الحظر بنجاح ❌", show_alert=True)
    
    await c.message.delete()
    await admin_manage_groups(c)
# ==========================================
# 5. نهاية الملف: ضمان التشغيل 24/7 (Keep-Alive)
# ==========================================
from aiohttp import web

# دالة الرد على "نغزة" المواقع الخارجية مثل Cron-job
async def handle_ping(request):
    return web.Response(text="Bot is Active and Running! 🚀")

if __name__ == '__main__':
    # 1. إعداد سيرفر ويب صغير في الخلفية للرد على طلبات الـ HTTP
    app = web.Application()
    app.router.add_get('/', handle_ping)
    
    loop = asyncio.get_event_loop()
    runner = web.AppRunner(app)
    loop.run_until_complete(runner.setup())
    
    # 2. تحديد المنفذ (Port): Render يستخدم غالباً 10000، و Koyeb يستخدم ما يحدده النظام
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    
    # تشغيل السيرفر كـ "مهمة" جانبية حتى لا يعطل البوت
    loop.create_task(site.start())
    print(f"✅ Keep-alive server started on port {port}")

    # 3. إعدادات السجلات والتشغيل النهائي للبوت
    logging.basicConfig(level=logging.INFO)
    
    # بدء استقبال الرسائل (Polling) مع تخطي التحديثات القديمة
    executor.start_polling(dp, skip_updates=True)

              
