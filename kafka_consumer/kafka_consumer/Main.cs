using Newtonsoft.Json;
using System;
using System.Data;
using System.Windows.Forms;

namespace t_kafka
{
    public partial class Main : Form
    {
        public Main()
        {
            InitializeComponent();
        }

        private void Main_Load(object sender, EventArgs e)
        {
            SetButtonEnabled(false);
        }

        private void SetButtonEnabled(bool state)
        {
            this.button2.Enabled = state;
            this.button3.Enabled = state;
        }

        private void Main_Shown(object sender, EventArgs e)
        {
            Consumer.GetInstance().GetMessageAction += MessageAction;
            Producer.GetInstance().SendMessageAction += MessageAction;
            this.button1.Click += ClickButton;
            this.button2.Click += ClickDev;
            this.button3.Click += ClearDataGrid;
        }

        private void ClearDataGrid(object sender, EventArgs e)
        {
            this.dataGridView1.Rows.Clear();
        }

        private void ClickDev(object sender, EventArgs e)
        {
            // Set dummy data
            DataTable dt = new DataTable("temp-table");
            dt.Columns.Add("datetime");
            dt.Columns.Add("value");
            dt.Rows.Add(System.DateTime.Now, 10);
            dt.Rows.Add(System.DateTime.Now, 20);
            dt.Rows.Add(System.DateTime.Now, 30);

            foreach (DataRow row in dt.Rows)
            {
                string json = JsonConvert.SerializeObject(row.ItemArray);
                json = string.Format("{{{0}}}", json.Substring(1, json.Length -2));
                Producer.GetInstance().AddMessage(json);
            }          
        }

        private void MessageAction(string message)
        {
            if (this.InvokeRequired)
            {
                this.BeginInvoke(new Action(()=> {
                    AppendMessage(message);
                }));
            }
            else
            {
                AppendMessage(message);
            }
        }

        private void AppendMessage(string message)
        {
            dataGridView1.Rows.Insert(0, System.DateTime.Now.ToString("HH:mm:ss.fff"), message);

            if (dataGridView1.Rows.Count > 100)
            {
                dataGridView1.Rows.RemoveAt(dataGridView1.Rows.Count - 1);
            }
        }

        private void ClickButton(object sender, EventArgs e)
        {
            string button_text = this.button1.Text;
            if (button_text == "시작")
            {
                // 마더버튼 제어 및 초기화
                this.button1.Text = "중지";
                string group_id = this.textBox1.Text;
                string server_path = this.textBox2.Text;
                // 컨트롤러 정지
                Consumer.GetInstance().SetConfig(group_id, server_path);
                Consumer.GetInstance().SetTopic(textBox3.Text);
                Consumer.GetInstance().Listen();
                Producer.GetInstance().SetConfig(server_path);
                Producer.GetInstance().SetTopic(textBox3.Text);
                Producer.GetInstance().Send();
                // 버튼 정지
                SetButtonEnabled(true);
            }
            else
            {
                // 마더버튼 제어
                this.button1.Text = "시작";
                // 컨트롤러 정지
                Consumer.GetInstance().Stop();
                Producer.GetInstance().Stop();
                // 버튼 정지
                SetButtonEnabled(false);
            }
        }

        private void Main_FormClosing(object sender, FormClosingEventArgs e)
        {
            Consumer.GetInstance().Stop();
            Producer.GetInstance().Stop();
        }
    }
}
